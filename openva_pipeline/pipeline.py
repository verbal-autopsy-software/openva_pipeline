#------------------------------------------------------------------------------#
# pipeline.py
#------------------------------------------------------------------------------#

import os
import csv
import datetime
from pandas import read_csv

from .transferDB import TransferDB
from .transferDB import DatabaseConnectionError
from .odk import ODK
from .openVA import OpenVA
from .dhis import DHIS

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
    logEvent(self, eventDesc, eventType)
        Inserts a message into the EventLog table of the Transfer database.
    config(self)
        Returns dictionary with the configuration settings for the classes
        Pipeline, ODK, OpenVA, and DHIS.
    runODK(slef, argsODK, workingDir)
        Run check duplicates, copy file, and briefcase.
    runOpenVA(self, argsOpenVA, argsPipeline, odkID, runDate)
        Create & run script or run smartva.
    runDHIS(self, argsDHIS, workingDir)
        Connect to API and post verbal autopsy records to DHIS server.
    storeResultsDB(self)
        Store VA results in Transfer database.
    closePipeline(self, conn)
        Update ODK_Conf ODKLastRun in Transfer DB and clean up files.

    """

    def __init__(self, dbFileName, dbDirectory, dbKey, useDHIS = True):

        self.dbFileName = dbFileName
        self.dbDirectory = dbDirectory
        self.dbKey = dbKey
        self.dbPath = os.path.join(dbDirectory, dbFileName)
        nowDate = datetime.datetime.now()
        self.pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")
        self.useDHIS = useDHIS

    def logEvent(self, eventDesc, eventType):
        """Commit event or error message to transfer database."""

        errorFile = os.path.join(self.dbDirectory, "dbErrorLog.csv")
        timeFMT = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        if os.path.isfile(errorFile) == False:
            try:
                with open(errorFile, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(["Date"] + 
                                    ["Description"] + 
                                    ["Additional Information"]
                    )
            except (OSError) as e:
                print(str(e) + "...Can't create dbErrorLog.csv")
                sys.exit(1)
        try:
            xferDB = TransferDB(dbFileName = self.dbFileName,
                                dbDirectory = self.dbDirectory,
                                dbKey = self.dbKey,
                                plRunDate = self.pipelineRunDate)
            conn = xferDB.connectDB()
            c = conn.cursor()
            sql = "INSERT INTO EventLog \
                   (eventDesc, eventType, eventTime) VALUES (?, ?, ?)"
            par = (eventDesc, eventType, timeFMT)
            c.execute(sql, par)
            conn.commit()
            conn.close()
        except (DatabaseConnectionError) as e:
            errorMsg = [timeFMT, str(e), "Committed by Pipeline.logEvent"]
            try:
                with open(errorFile, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(errorMsg)
            except:
                print("Can't write to dbErrorLog.csv")
                print(errorMsg)

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

    def runODK(self, argsODK, argsPipeline):
        """Run check duplicates, copy file, and briefcase."""
        pipelineODK = ODK(argsODK, argsPipeline.workingDirectory)
        pipelineODK.mergeToPrevExport()
        odkBC = pipelineODK.briefcase()
        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = self.dbDirectory,
                            dbKey = self.dbKey,
                            plRunDate = self.pipelineRunDate)
        conn = xferDB.connectDB()
        xferDB.configPipeline(conn)
        xferDB.checkDuplicates(conn)
        conn.close()
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

    def runDHIS(self, argsDHIS, argsPipeline):
        """Connect to API and post events."""
        pipelineDHIS = DHIS(argsDHIS, argsPipeline.workingDirectory)
        apiDHIS = pipelineDHIS.connect()
        postLog = pipelineDHIS.postVA(apiDHIS)
        pipelineDHIS.verifyPost(postLog, apiDHIS)

        dhisOut = {"vaProgramUID": pipelineDHIS.vaProgramUID,
                   "postLog": postLog,
                   "nPostedRecords": pipelineDHIS.nPostedRecords}
        return(dhisOut)

    def storeResultsDB(self):
        """Store VA results in Transfer database."""
        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = self.dbDirectory,
                            dbKey = self.dbKey,
                            plRunDate = self.pipelineRunDate)
        conn = xferDB.connectDB()
        xferDB.configPipeline(conn)
        xferDB.storeVA(conn)
        conn.close()

    def closePipeline(self):
        """Update ODK_Conf ODKLastRun in Transfer DB and clean up files."""
        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = self.dbDirectory,
                            dbKey = self.dbKey,
                            plRunDate = self.pipelineRunDate)
        conn = xferDB.connectDB()
        xferDB.configPipeline(conn)
        xferDB.cleanODK()
        xferDB.cleanOpenVA()
        xferDB.cleanDHIS()
        xferDB.updateODKLastRun(conn, self.pipelineRunDate)
        conn.close()
