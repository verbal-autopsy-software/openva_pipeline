#------------------------------------------------------------------------------#
# runPipeline.py
#------------------------------------------------------------------------------#

import sys
from pipeline import Pipeline
from transferDB import PipelineError
from transferDB import DatabaseConnectionError
from transferDB import PipelineConfigurationError
from transferDB import ODKConfigurationError
from transferDB import OpenVAConfigurationError
from transferDB import DHISConfigurationError
from odk import ODKError
from openVA import OpenVAError
from openVA import SmartVAError
from dhis import DHISError

def runPipeline(database_file_name,
                database_directory,
                database_key,
                export_to_DHIS = True):

    pl = Pipeline(dbFileName = database_file_name,
                  dbDirectory = database_directory,
                  dbKey = database_key,
                  useDHIS = export_to_DHIS)

    # get configuration settings for the pipeline
    try:
        settings = pl.config()
    except PipelineConfigurationError as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    settingsPipeline = settings["pipeline"]
    settingsODK = settings["odk"]
    settingsOpenVA = settings["openVA"]
    settingsDHIS = settings["dhis"]

    # retrieve records from ODK
    try:
        odkBC = pl.runODK(settingsODK,
                          settingsPipeline)
        pl.logEvent("Briefcase Export Completed Successfully", "Event")
    except ODKError as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    # run openVA
    try:
        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)
        pl.logEvent("OpenVA Analysis Completed Successfully", "Event")
    except (OpenVAError, SmartVAError) as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    # run DHIS
    if (export_to_DHIS):
        try:
            pipelineDHIS = pl.runDHIS(settingsDHIS,
                                      settingsPipeline)
            pl.logEvent("Posted Events to DHIS2 Successfully", "Event")
        except DHISError as e:
            pl.logEvent(str(e), "Error")
            sys.exit(1)

    # store results
    try:
        pl.storeResultsDB()
        pl.logEvent("Stored Records to Xfer Database Successfully", "Event")
    except (PipelineError, DatabaseConnectionError, 
            PipelineConfigurationError) as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)

    # clean up files and update database
    try:
        pl.closePipeline()
        pl.logEvent("Successfully Completed Run of Pipeline", "Event")
    except (DatabaseConnectionError, DatabaseConnectionError) as e:
        pl.logEvent(str(e), "Error")
        sys.exit(1)


if __name__ == "__main__":
    runPipeline(database_file_name= "run_Pipeline.db", 
                database_directory = "tests",
                database_key = "enilepiP")
