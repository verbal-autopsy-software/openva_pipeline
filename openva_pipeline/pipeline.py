#------------------------------------------------------------------------------#
# pipeline.py
#------------------------------------------------------------------------------#

import os
import datetime
from transferDB import TransferDB
from odk import ODK
from openVA import OpenVA
from dhis import DHIS

class Pipeline:
    """Primary API for the openVA pipeline.

    This class calls three others to move verbal autopsy data from an ODK
    Aggregate server (using the ODK class), through the openVA R package to
    assign cause of death (using the OpenVA class), and deposits the VA records
    with assigned causes to either/both a DHIS server (using the DHIS class) or
    the Transfer database -- a local database which also contains configuration
    settings for the pipeline.  The TransferDB class performs the final step of
    storing the results locally as well as accessing the configuration settings.

    Parameters
    ----------
    dbFileName : str
        File name of the Tranfser database.
    dbDirectory : str
        Path of folder containing the Transfer database.
    dbKey : str
        Encryption key for the Transfer database.

    Methods
    -------
    config(self)
        Returns dictionary with the configuration settings for the classes
        Pipeline, ODK, OpenVA, and DHIS.

    """

    def __init__(self, dbFileName, dbDirectory, dbKey, useDHIS = True):

        self.dbFileName = dbFileName
        self.dbDirectory = dbDirectory
        self.dbKey = dbKey
        self.dbPath = os.path.join(dbDirectory, dbFileName)
        nowDate = datetime.datetime.now()
        self.pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")
        self.useDHIS = useDHIS

    def config(self):
        """Return dictionary with configuration settings for pipeline steps."""

        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = self.dbDirectory,
                            dbKey = self.dbKey,
                            plRunDate = self.pipelineRunDate)
        conn = xferDB.connectDB()
        settingsPipeline = xferDB.configPipeline(conn)
        settingsODK = xferDB.configODK(conn)
        settingsOpenVA = xferDB.configOpenVA(conn,
                                             settingsPipeline.algorithm,
                                             settingsPipeline.workingDirectory)
        settings = {"pipeline": settingsPipeline,
                    "odk": settingsODK,
                    "openVA": settingsOpenVA}
        if self.useDHIS:
            settingsDHIS = xferDB.configDHIS(conn,
                                             settingsPipeline.algorithm)
            settings["dhis"] = settingsDHIS
        conn.close()
        return(settings)

    def runODK(self, argsODK, workingDir):
        """Run check duplicates, copy file, and briefcase."""
        # xferDB = TransferDB(dbFileName = self.dbFileName,
        #                     dbDirectory = self.dbDirectory,
        #                     dbKey = self.dbKey,
        #                     plRunDate = self.pipelineRunDate)
        # conn = xferDB.connectDB()
        pipelineODK = ODK(argsODK, workingDir)
        pipelineODK.mergeToPrevExport()
        odkBC = pipelineODK.briefcase()
        # HERE -- check duplictes
        return(odkBC)

    def runOpenVA(self, argsOpenVA, argsPipeline, odkID, runDate):
        """Create & run script or run smartva."""

        pipelineOpenVA = OpenVA(vaArgs = argsOpenVA,
                                pipelineArgs = argsPipeline,
                                odkID = odkID,
                                runDate = runDate)
        zeroRecords = pipelineOpenVA.copyVA()
        rOut = {"zeroRecords": zeroRecords}
        if not zeroRecords:
            pipelineOpenVA.rScript()
            completed = pipelineOpenVA.getCOD()
            rOut["completed"] = completed
        return(rOut)

    def runDHIS(self, argsDHIS, workingDir):
        """Connect to API and post events."""
        pipelineDHIS = DHIS(argsDHIS, workingDir)
        apiDHIS = pipelineDHIS.connect()
        postLog = pipelineDHIS.postVA(apiDHIS)
        pipelineDHIS.verifyPost(postLog, apiDHIS)

        dhisOut = {"vaProgramUID": pipelineDHIS.vaProgramUID,
                   "postLog": postLog,
                   "nPostedRecords": pipelineDHIS.nPostedRecords}
        return(dhisOut)

    def depositResults(self):
        """StoreVA (not sure if this is needed)."""
        # xferDB = TransferDB(dbFileName = self.dbFileName,
        #                     dbDirectory = self.dbDirectory,
        #                     dbKey = self.dbKey,
        #                     plRunDate = self.pipelineRunDate)
        # conn = xferDB.connectDB()
        pass

    def closePipeline(self, conn):
        """Run updateODKLastRun and clean up files (close conn?)."""
        pass
