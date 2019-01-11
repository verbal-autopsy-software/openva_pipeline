"""
openva_pipeline.pipeline
------------------------

This module defines the primary API for the openVA Pipeline.
"""

import os
import csv
import datetime
import requests
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

    :param dbFileName: File name of the Tranfser database.
    :type dbFileName: string
    :param dbDirectory: str
        Path of folder containing the Transfer database.
    :type dbDirectory: string
    :param dbKey: Encryption key for the Transfer database.
    :type dbKey: string
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
        """Commit event or error message into EventLog table of transfer database.

        :param eventDesc: Description of the event.
        :type eventDesc: string
        :param eventType: Type of event (error or information)
        :type evenType: string
        """

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
        """Fetch configuration settings from Transfer DB.

        This method queries the Transfer database (DB) and returns objects that
        can be used as the arguments for other methods in this class, i.e.,
        :meth:`Pipeline.runODK() <runODK>`, 
        :meth:`Pipeline.runOpenVA() <runOpenVA>`, and
        :meth:`Pipeline.runDHIS() <runDHIS>`.

        :param dbFileName: File name of the Transfer DB.
          (e.g., Pipeline.db)
        :type dbFileName: str
        :param dbDirectory: Path to the location of the Transfer DB.
        :type dbDirectory: str
        :param dbKey: Encryption key for the Transfer DB
        :type dbKey: str
        :param plRunDate: Date when pipeline started latest 
          run (YYYY-MM-DD_hh:mm:ss).
        :type plRunDate: date
        :returns: Configuration settings for pipeline steps (e.g. connecting 
          to ODK Aggregate, running openVA, or posting records to DHIS)
        :rtype: dictionary
        """

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
        """Run check duplicates, copy file, and briefcase.

        This method runs the Java application ODK Briefcase,
        by calling the method
        :meth:`ODK.briefcase() <openva_pipeline.odk.ODK.briefcase>`
        where the configuration settings are taken from the argument
        argsODK (see :meth:`Pipeline.config() <config>`)
        , and downloads verbal autopsy (VA)
        records as a (csv) export from an ODK Aggregate server.  If there
        is a previous ODK export file, this method merges the files by
        keeping only the unique VA records.

        :param argsODK: Arguments passed to ODK Briefcase for connecting
          to an ODK Aggregate server.
        :type argsODK: named tuple
        :param argsPipeline: Arguments for configuration the openva pipeline.
        :type argsPipeline: named tuple
        :return: Return value from method subprocess.run()
        :rtype: subprocess.CompletedProcess
        """

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
        """Create & run script or run smartva.

        This method runs the through the suite of methods in the 
        :class:`OpenVA <openva_pipeline.openVA.OpenVA>`.
        class.  The list of tasks performed (in order) are: (1) call the method
        :meth:`OpenVA.copyVA() <openva_pipeline.openVA.OpenVA.copyVA>`
        to copy over CSV files with VA data (retrieved from ODK Aggregate);
        (2) use the method
        :meth:`OpenVA.rScript() <openva_pipeline.openVA.OpenVA.rScript>`
        to create an R script; and (3) call the method
        :meth:`OpenVA.getCOD() <openva_pipeline.openVA.OpenVA.getCOD>` to
        run the R script that estimates the causes of death and stores the
        results in "OpenVAFiles/recordStorage.csv" and 
        "OpenVAFiles/entityAttributeValue.csv" (the former serving as the
        blob posted to DHIS2).

        :param argsOpenVA: Configuration settings for openVA.
        :type argsOpenVA: named tuple
        :param argsPipeline: Configuration settings for OpenVA Pipeline
        :type argsPipeline: named tuple
        :param odkID: column/variable name of VA record ID in ODK export
        :type odkID: string
        :param runDate: date and time when OpenVA Pipeline ran
        :type runDate: nowDate.strftime("%Y-%m-%d_%H:%M:%S")
        :return: an indicator of zero VA records in the ODK export
        :rtype: dictionary
        """

        pipelineOpenVA = OpenVA(vaArgs = argsOpenVA,
                                pipelineArgs = argsPipeline,
                                odkID = odkID,
                                runDate = runDate)
        zeroRecords = pipelineOpenVA.copyVA()
        rOut = {"zeroRecords": zeroRecords}
        if not zeroRecords:
            pipelineOpenVA.rScript()
            completed = pipelineOpenVA.getCOD()
            # rOut["completed"] = completed
            rOut["returncode"] = completed.returncode
        return(rOut)

    def runDHIS(self, argsDHIS, argsPipeline):
        """Connect to API and post events.

        This method first calls the method
        :meth:`DHIS.connect() <openva_pipeline.dhis.DHIS.connect>`
        to establish a connection with a DHIS2 server and, second
        calls the method 
        :meth:`DHIS.postVA() <openva_pipeline.dhis.DHIS.postVA>` to
        post VA data, the assigned causes of death, and associated
        metadata (concerning cause assignment).

        :param argsDHIS: Configuration settings for connecting to DHIS2 server.
        :type argsOpenVA: named tuple
        :param argsPipeline: Configuration settings for OpenVA Pipeline
        :type argsPipeline: named tuple
        :return: VA Program ID from the DHIS2 server, the log from 
          the DHIS2 connection, and the number of records posted to DHIS2
        :rtype: dictionary
        """

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
        """Update ODK_Conf ODKLastRun in Transfer DB and clean up files.

        This method calls methods in the
        :class:`TransferDB <openva_pipeline.transferDB.TransferDB>`
        class to remove the data files created at each step of the 
        pipeline.  More specifically, it runs
        :meth:`TransferDB.cleanODK() <openva_pipeline.transferDB.TransferDB.cleanODK>`
        to remove the ODK Briefcase export files ("ODKFiles/odkBCExportNew.csv"
        and "ODKFiles/odkBCExportPrev.csv") if they exist;
        :meth:`TransferDB.cleanOpenVA() <openva_pipeline.transferDB.TransferDB.cleanOpenVA>`
        to remove the input data file ("OpenVAFiles/openVA_input.csv") and the
        output files ("OpenVAFiles/recordStorage.csv",
        "OpenVAFiles/newStorage.csv", and
        "OpenVAFiles/entityAttributeValue.csv") -- note that all of these
        results are stored in either/both of the Transfer DB and the DHIS2
        server's VA program; and, third, the method
        :meth:`TransferDB.cleanDHIS() <openva_pipeline.transferDB.TransferDB.cleanDHIS>`
        is called to remove the blobs posted to the DHIS2 server and stored in the
        folder "DHIS/blobs".  Finally, this method updates the Transfer DB's
        value in the ODK_Conf table's variable odkLastRun so the next ODK
        Export file does not include VA records already processed through the
        pipeline.
        """

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
