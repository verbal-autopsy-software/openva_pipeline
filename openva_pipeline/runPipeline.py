#------------------------------------------------------------------------------#
#    Copyright (C) 2018  Jason Thomas, Samuel J. Clark, & Martin Bratschi
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#------------------------------------------------------------------------------#

import sys
import os
import requests
from pysqlcipher3 import dbapi2 as sqlcipher

from openva_pipeline.pipeline import Pipeline
from openva_pipeline.exceptions import PipelineError
from openva_pipeline.exceptions import DatabaseConnectionError
from openva_pipeline.exceptions import PipelineConfigurationError
from openva_pipeline.exceptions import ODKConfigurationError
from openva_pipeline.exceptions import OpenVAConfigurationError
from openva_pipeline.exceptions import DHISConfigurationError
from openva_pipeline.exceptions import ODKError
from openva_pipeline.exceptions import OpenVAError
from openva_pipeline.exceptions import SmartVAError
from openva_pipeline.exceptions import DHISError

def createTransferDB(database_file_name,
                     database_directory,
                     database_key):
    """Create the (SQLite encrypted) Transfer Database.

    :param database_file_name: File name for the Transfer Database.
    :param database_directory: Path of the Transfer Database.
    :param datatbase_key: Encryption key for the Transfer Database
    :param export_to_DHIS: Indicator for posting VA records to a DHIS2 server.
    """

    dbPath = os.path.join(database_directory, database_file_name)

    sqlPath = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "sql/pipelineDB.sql")
    )
    try:
        conn = sqlcipher.connect(dbPath)
    except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
        raise DatabaseConnectionError("Unable to create database..." + str(e))
    try:
        parSetKey = "\"" + database_key + "\""
        conn.execute("PRAGMA key = " + parSetKey)
        # c = conn.cursor()
    except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
        raise DatabaseConnectionError("Unable to set encryption key..." + str(e))
    try:
        with open(sqlPath, "r", newline = "\n") as sqlScript:
            # c.executescript(sqlScript.read())
            conn.executescript(sqlScript.read())
    except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
        raise DatabaseConnectionError \
            ("Problem running script (pipelineDB.sql)..." + str(e))

def runPipeline(database_file_name,
                database_directory,
                database_key,
                export_to_DHIS = True):
    """Runs through all steps of the OpenVA Pipeline

    This function is a wrapper for the Pipeline class, which
    runs through all steps of the OpenVA Pipeline -- (1) connect to
    Transfer Database (to retrieve configuration settings); (2) connect to
    ODK Aggregate to download a CSV file with VA records; (3) run openVA
    (or SmartVA) to assign cause of death; and (4) store CoD results and
    VA data in the Transfer Database as well as a DHIS2 VA Program (if
    requested).

    :param database_file_name: File name for the Transfer Database.
    :param database_directory: Path of the Transfer Database.
    :param datatbase_key: Encryption key for the Transfer Database
    :param export_to_DHIS: Indicator for posting VA records to a DHIS2 server.
    :type export_to_DHIS: (Boolean)
    """

    pl = Pipeline(dbFileName = database_file_name,
                  dbDirectory = database_directory,
                  dbKey = database_key,
                  useDHIS = export_to_DHIS)
    try:
        settings = pl.config()
    except PipelineConfigurationError as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    settingsPipeline = settings["pipeline"]
    settingsODK = settings["odk"]
    settingsOpenVA = settings["openVA"]
    settingsDHIS = settings["dhis"]

    try:
        odkBC = pl.runODK(settingsODK,
                          settingsPipeline)
        pl.logEvent("Briefcase Export Completed Successfully", "Event")
    except ODKError as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    try:
        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)
        pl.logEvent("OpenVA Analysis Completed Successfully", "Event")
    except (OpenVAError, SmartVAError) as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    if (export_to_DHIS):
        try:
            pipelineDHIS = pl.runDHIS(settingsDHIS,
                                      settingsPipeline)
            pl.logEvent("Posted Events to DHIS2 Successfully", "Event")
        except DHISError as e:
            pl.logEvent(str(e), "Error")
            sys.exit(1)

    try:
        pl.storeResultsDB()
        pl.logEvent("Stored Records to Xfer Database Successfully", "Event")
    except (PipelineError, DatabaseConnectionError,
            PipelineConfigurationError) as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    try:
        pl.closePipeline()
        pl.logEvent("Successfully Completed Run of Pipeline", "Event")
    except (DatabaseConnectionError, DatabaseConnectionError) as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

def downloadBriefcase():
    """Download the ODK Briefcase (v1.12.2) jar file from Git Hub."""

    bcURL = "https://github.com/opendatakit/briefcase/releases/download/v1.12.2/ODK-Briefcase-v1.12.2.jar"
    try:
        with open("ODK-Briefcase-v1.12.2.jar", "wb") as bcFile:
            r = requests.get(bcURL)
            bcFile.write(r.content)
        os.chmod("ODK-Briefcase-v1.12.2.jar", 0o744)
    except (requests.RequestException, IOError) as e:
        raise ODKError("Error downloading Briefcase: {}".format(str(e)))

def downloadSmartVA():
    """Download the smartva (linux) binary application file from Git Hub."""

    smartvaURL = "https://github.com/ihmeuw/SmartVA-Analyze/releases/download/v2.0.0/smartva"
    try:
        with open("smartva", "wb") as smartvaBinary:
            r = requests.get(smartvaURL)
            smartvaBinary.write(r.content)
        os.chmod("smartva", 0o777)
    except (requests.RequestException, IOError) as e:
        raise ODKError("Error downloading smartva: {}".format(str(e)))

# if __name__ == "__main__":
#     runPipeline(database_file_name= "run_Pipeline.db",
#                 database_directory = "tests",
#                 database_key = "enilepiP")
