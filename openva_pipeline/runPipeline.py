# ------------------------------------------------------------------------------#
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
# ------------------------------------------------------------------------------#

import sys
import os
import requests
from pysqlcipher3 import dbapi2 as sqlcipher

from openva_pipeline.pipeline import Pipeline
from openva_pipeline.exceptions import PipelineError
from openva_pipeline.exceptions import DatabaseConnectionError
from openva_pipeline.exceptions import PipelineConfigurationError
from openva_pipeline.exceptions import ODKError
from openva_pipeline.exceptions import OpenVAError
from openva_pipeline.exceptions import SmartVAError
from openva_pipeline.exceptions import DHISError


def create_transfer_db(database_file_name, database_directory, database_key):
    """Create the (SQLite encrypted) Transfer Database.

    :param database_file_name: File name for the Transfer Database.
    :param database_directory: Path of the Transfer Database.
    :param database_key: Encryption key for the Transfer Database
    """

    db_path = os.path.join(database_directory, database_file_name)

    sql_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "sql/pipelineDB.sql")
    )
    try:
        conn = sqlcipher.connect(db_path)
    except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
        raise DatabaseConnectionError("Unable to create database..." + str(e))
    try:
        par_set_key = '"' + database_key + '"'
        conn.execute("PRAGMA key = " + par_set_key)
    except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
        raise DatabaseConnectionError("Unable to set encryption key..." +
                                      str(e))
    try:
        with open(sql_path, "r", newline="\n", encoding="utf-8") as sqlScript:
            conn.executescript(sqlScript.read())
    except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
        raise DatabaseConnectionError(
            "Problem running script (pipelineDB.sql)..." + str(e)
        )


def run_pipeline(
        database_file_name, database_directory, database_key,
        export_to_dhis=True):
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
    :param database_key: Encryption key for the Transfer Database
    :param export_to_dhis: Indicator for posting VA records to a DHIS2 server.
    :type export_to_dhis: (Boolean)
    """

    pl = Pipeline(
        db_file_name=database_file_name,
        db_directory=database_directory,
        db_key=database_key,
        use_dhis=export_to_dhis,
    )
    try:
        settings = pl.config()
    except PipelineConfigurationError as e:
        pl.log_event(str(e), "Error")
        sys.exit(1)

    try:
        pl.run_odk(settings)
        pl.log_event("ODK Export Completed Successfully", "Event")
    except ODKError as e:
        pl.log_event(str(e), "Error")
        sys.exit(1)

    try:
        r_out = pl.run_openva(settings)
        pl.log_event("OpenVA Analysis Completed Successfully", "Event")
    except (OpenVAError, SmartVAError) as e:
        pl.log_event(str(e), "Error")
        sys.exit(1)

    if r_out["zero_records"]:
        pl.log_event("No new VA records from ODK (now exiting)", "Event")
        sys.exit(0)

    if export_to_dhis:
        try:
            pl.run_dhis(settings)
            pl.log_event("Posted Events to DHIS2 Successfully", "Event")
        except DHISError as e:
            pl.log_event(str(e), "Error")
            sys.exit(1)

    try:
        pl.store_results_db()
        pl.log_event("Stored Records to Xfer Database Successfully", "Event")
    except (PipelineError, DatabaseConnectionError,
            PipelineConfigurationError) as e:
        pl.log_event(str(e), "Error")
        sys.exit(1)

    try:
        pl.close_pipeline()
        pl.log_event("Successfully Completed Run of Pipeline", "Event")
    except (DatabaseConnectionError, DatabaseConnectionError) as e:
        pl.log_event(str(e), "Error")
        sys.exit(1)


def download_briefcase():
    """Download the ODK Briefcase (v1.18.0) jar file from Git Hub."""

    bc_url = ("https://github.com/getodk/briefcase/releases/download/" +
              "v1.18.0/ODK-Briefcase-v1.18.0.jar")
    try:
        with open("ODK-Briefcase-v1.18.0.jar", "wb") as bc_file:
            r = requests.get(bc_url)
            bc_file.write(r.content)
        os.chmod("ODK-Briefcase-v1.18.0.jar", 0o744)
    except (requests.RequestException, IOError) as e:
        raise ODKError("Error downloading Briefcase: {}".format(str(e)))


def download_smartva():
    """Download the smartva (linux) binary application file from Git Hub."""

    smartva_url = (
        "https://github.com/ihmeuw/SmartVA-Analyze/releases/download/" +
        "v2.0.0/smartva"
    )
    try:
        with open("smartva", "wb") as smartva_binary:
            r = requests.get(smartva_url)
            smartva_binary.write(r.content)
        os.chmod("smartva", 0o777)
    except (requests.RequestException, IOError) as e:
        raise SmartVAError("Error downloading smartva: {}".format(str(e)))

# if __name__ == "__main__":
#     runPipeline(database_file_name= "run_Pipeline.db",
#                 database_directory = "tests",
#                 database_key = "enilepiP")
