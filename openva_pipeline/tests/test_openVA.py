#------------------------------------------------------------------------------#
# test_openVA.py
#------------------------------------------------------------------------------#

import unittest
import os
import shutil
from context import pipeline
from context import transferDB
from context import openVA
from transferDB import TransferDB
from openVA import OpenVA
from pysqlcipher3 import dbapi2 as sqlcipher
import datetime

os.chdir(os.path.abspath(os.path.dirname(__file__)))

class Check_1_importVA(unittest.TestCase):

    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    dirODK = "ODKFiles"
    dirOpenVA = "OpenVAFiles"
    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey)
    conn = xferDB.connectDB()
    settingsPipeline = xferDB.configPipeline(conn)
    settingsODK = xferDB.configODK(conn)
    settingsInterVA = xferDB.configOpenVA(conn,
                                          "InterVA",
                                          settingsPipeline.workingDirectory)
    runDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
      strftime("%Y_%m_%d_%H:%M:%S")

    openVA = OpenVA(vaArgs = settingsInterVA,
                    pipelineArgs = settingsPipeline,
                    odkID = settingsODK.odkID,
                    runDate = runDate)

    def test_1_importVA_isFile(self):
        """Check that importVA() brings in new file."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")

        zeroRecords = self.openVA.copyVA()

        self.assertTrue(
            os.path.isfile(self.dirOpenVA + "/openVA_input.csv")
        )
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")
        os.remove(self.dirOpenVA + "/openVA_input.csv")

    def test_1_importVA_merge(self):
        """Check that importVA() includes all records."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")

        zeroRecords = self.openVA.copyVA()

        hasAll = True
        with open("OpenVAFiles/openVA_input.csv") as fCombined:
            fCombinedLines = fCombined.readlines()
        with open("ODKFiles/previous_bc_export.csv") as fPrevious:
            fPreviousLines = fPrevious.readlines()
        with open("ODKFiles/another_bc_export.csv") as fAnother:
            fAnotherLines = fAnother.readlines()
        for line in fPreviousLines:
            if line not in fCombinedLines:
                hasAll = False
        for line in fAnotherLines:
            if line not in fCombinedLines:
                hasAll = False
        self.assertTrue(hasAll)
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")
        os.remove(self.dirOpenVA + "/openVA_input.csv")

    def test_1_importVA_zeroRecords_1(self):
        """Check that importVA() returns zeroRecords == True."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/zeroRecords_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/zeroRecords_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirOpenVA + "/openVA_input.csv"):
            os.remove(self.dirOpenVA + "/openVA_input.csv")

        zeroRecords = self.openVA.copyVA()

        self.assertTrue(zeroRecords)
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")

    def test_1_importVA_zeroRecords_2(self):
        """Check that importVA() does not produce file if zero records."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirOpenVA + "/openVA_input.csv"):
            os.remove(self.dirOpenVA + "/openVA_input.csv")

        zeroRecords = self.openVA.copyVA()

        self.assertFalse(zeroRecords)
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")
        os.remove(self.dirOpenVA + "/openVA_input.csv")
      
    def test_1_importVA_zeroRecords_3(self):
        """Check that importVA() doesn't create new file if returns zeroRecords == True."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/zeroRecords_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/zeroRecords_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirOpenVA + "/openVA_input.csv"):
            os.remove(self.dirOpenVA + "/openVA_input.csv")

        zeroRecords = self.openVA.copyVA()

        self.assertFalse(
            os.path.isfile(self.dirOpenVA + "/openVA_input.csv")
        )
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")

        # raise error if no ODK files?

class Check_2_rScript(unittest.TestCase):

    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    dbDirectory = "."
    xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                        dbDirectory = dbDirectory,
                        dbKey = dbKey)
    conn = xferDB.connectDB()
    c = conn.cursor()
    sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
    par = ("InSilicoVA","InsilicoVA|1.1.4|Custom|1|2012 WHO Verbal Autopsy Form|RC1")
    c.execute(sql, par)
    settingsPipeline = xferDB.configPipeline(conn)
    settingsODK = xferDB.configODK(conn)
    settingsInSilicoVA = xferDB.configOpenVA(conn,
                                             "InSilicoVA",
                                             settingsPipeline.workingDirectory)
    conn.rollback()
    dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
    dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
    runDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
      strftime("%Y_%m_%d_%H:%M:%S")
    rScriptFile = os.path.join(dirOpenVA, runDate,
                               "Rscript_" + runDate + ".R")
    
    openVA = OpenVA(vaArgs = settingsInSilicoVA,
                    pipelineArgs = settingsPipeline,
                    odkID = settingsODK.odkID,
                    runDate = runDate)

    def test_2_rScript_insilico(self):
        """Check that rScript() creates an R script for InSilicoVA."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")

        zeroRecords = self.openVA.copyVA()
        self.openVA.rScript()

        self.assertTrue(os.path.isfile(self.rScriptFile))
        shutil.rmtree(
            os.path.join(self.dirOpenVA, self.runDate),
            ignore_errors = True
        )

if __name__ == "__main__":
    unittest.main()
