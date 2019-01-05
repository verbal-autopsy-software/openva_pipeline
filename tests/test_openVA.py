import unittest
import os
import shutil
import collections
from pysqlcipher3 import dbapi2 as sqlcipher
import datetime

import context
from openva_pipeline.transferDB import TransferDB
from openva_pipeline.openVA import OpenVA
from openva_pipeline.exceptions import OpenVAError
from openva_pipeline.exceptions import SmartVAError

os.chdir(os.path.abspath(os.path.dirname(__file__)))

class Check_1_copyVA(unittest.TestCase):

    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    dirODK = "ODKFiles"
    dirOpenVA = "OpenVAFiles"
    pipelineRunDate = datetime.datetime.now()
    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    settingsPipeline = xferDB.configPipeline(conn)
    settingsODK = xferDB.configODK(conn)
    settingsInterVA = xferDB.configOpenVA(conn,
                                          "InterVA",
                                          settingsPipeline.workingDirectory)
    staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
      strftime("%Y_%m_%d_%H:%M:%S")

    shutil.rmtree(
        os.path.join(dirOpenVA, staticRunDate),
        ignore_errors = True
    )

    rOpenVA = OpenVA(vaArgs = settingsInterVA,
                     pipelineArgs = settingsPipeline,
                     odkID = settingsODK.odkID,
                     runDate = staticRunDate)

    def test_1_copyVA_isFile(self):
        """Check that copyVA() brings in new file."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")

        zeroRecords = self.rOpenVA.copyVA()

        self.assertTrue(
            os.path.isfile(self.dirOpenVA + "/openVA_input.csv")
        )
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")
        os.remove(self.dirOpenVA + "/openVA_input.csv")

    def test_1_copyVA_merge(self):
        """Check that copyVA() includes all records."""

        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")

        zeroRecords = self.rOpenVA.copyVA()

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

    def test_1_copyVA_zeroRecords_1(self):
        """Check that copyVA() returns zeroRecords == True."""

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

        zeroRecords = self.rOpenVA.copyVA()

        self.assertTrue(zeroRecords)
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")

    def test_1_copyVA_zeroRecords_2(self):
        """Check that copyVA() does not produce file if zero records."""

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

        zeroRecords = self.rOpenVA.copyVA()

        self.assertFalse(zeroRecords)
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")
        os.remove(self.dirOpenVA + "/openVA_input.csv")
      
    def test_1_copyVA_zeroRecords_3(self):
        """Check that copyVA() doesn't create new file if returns zeroRecords == True."""

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

        zeroRecords = self.rOpenVA.copyVA()

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
    pipelineRunDate = datetime.datetime.now()

    xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()

    def test_2_rScript_insilico(self):
        """Check that rScript() creates an R script for InSilicoVA."""

        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("InSilicoVA", "InSilicoVA|1.1.4|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        settingsInSilicoVA = self.xferDB.configOpenVA(self.conn,
                                                      "InSilicoVA",
                                                      settingsPipeline.workingDirectory)
        self.conn.rollback()
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                        strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        rScriptFile = os.path.join(dirOpenVA, staticRunDate,
                                   "Rscript_" + staticRunDate + ".R")
        rOpenVA = OpenVA(vaArgs = settingsInSilicoVA,
                         pipelineArgs = settingsPipeline,
                         odkID = settingsODK.odkID,
                         runDate = staticRunDate)

        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/previous_bc_export.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/another_bc_export.csv",
                    dirODK + "/odkBCExportNew.csv")

        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()

        self.assertTrue(os.path.isfile(rScriptFile))
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

    def test_2_rScript_interva(self):
        """Check that rScript() creates an R script for InterVA."""

        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("InterVA","InterVA4|4.04|InterVA|4|2016 WHO Verbal Autopsy Form|v1_4_1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        settingsInSilicoVA = self.xferDB.configOpenVA(self.conn,
                                                      "InterVA",
                                                      settingsPipeline.workingDirectory)
        self.conn.rollback()
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                  strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        rScriptFile = os.path.join(dirOpenVA, staticRunDate,
                                   "Rscript_" + staticRunDate + ".R")
        rOpenVA = OpenVA(vaArgs = settingsInSilicoVA,
                         pipelineArgs = settingsPipeline,
                         odkID = settingsODK.odkID,
                         runDate = staticRunDate)
        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/previous_bc_export.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/another_bc_export.csv",
                    dirODK + "/odkBCExportNew.csv")
        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()
        self.assertTrue(os.path.isfile(rScriptFile))
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

class Check_3_getCOD(unittest.TestCase):

    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()
    dirODK = "ODKFiles"
    dirOpenVA = "OpenVAFiles"
    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()

    settingsPipeline = xferDB.configPipeline(conn)
    settingsODK = xferDB.configODK(conn)
    settingsInterVA = xferDB.configOpenVA(conn,
                                          "InterVA",
                                          settingsPipeline.workingDirectory)
    staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                    strftime("%Y_%m_%d_%H:%M:%S")

    def test_3_getCOD_insilico(self):
        """Check that getCOD() executes R script for insilico"""
        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("InSilicoVA", "InSilicoVA|1.1.4|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        settingsInSilicoVA = self.xferDB.configOpenVA(self.conn,
                                                      "InSilicoVA",
                                                      settingsPipeline.workingDirectory)
        self.conn.rollback()
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                        strftime("%Y_%m_%d_%H:%M:%S")
        rOutFile = os.path.join(dirOpenVA, staticRunDate,
                                "Rscript_" + staticRunDate + ".Rout")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        rOpenVA = OpenVA(vaArgs = settingsInSilicoVA,
                         pipelineArgs = settingsPipeline,
                         odkID = settingsODK.odkID,
                         runDate = staticRunDate)

        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/previous_bc_export.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/another_bc_export.csv",
                    dirODK + "/odkBCExportNew.csv")

        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()
        rOpenVA.getCOD()

        self.assertTrue(os.path.isfile(rOutFile))
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

    def test_3_getCOD_insilico_exception(self):
        """getCOD() raises exception with faulty R script for InSilicoVA."""
        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("InSilicoVA", "InSilicoVA|1.1.4|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        settingsInSilicoVA = self.xferDB.configOpenVA(self.conn,
                                                      "InSilicoVA",
                                                      settingsPipeline.workingDirectory)
        self.conn.rollback()
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                        strftime("%Y_%m_%d_%H:%M:%S")
        rOutFile = os.path.join(dirOpenVA, staticRunDate,
                                "Rscript_" + staticRunDate + ".Rout")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        rOpenVA = OpenVA(vaArgs = settingsInSilicoVA,
                         pipelineArgs = settingsPipeline,
                         odkID = "this should raise an exception",
                         runDate = staticRunDate)

        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/previous_bc_export.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/another_bc_export.csv",
                    dirODK + "/odkBCExportNew.csv")

        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()

        self.assertRaises(OpenVAError, rOpenVA.getCOD)
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

    def test_3_getCOD_interva(self):
        """Check that getCOD() executes R script for interva"""
        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("InterVA", "InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        settingsInterVA = self.xferDB.configOpenVA(self.conn,
                                                   "InterVA",
                                                   settingsPipeline.workingDirectory)
        self.conn.rollback()
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                        strftime("%Y_%m_%d_%H:%M:%S")
        rOutFile = os.path.join(dirOpenVA, staticRunDate,
                                "Rscript_" + staticRunDate + ".Rout")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        rOpenVA = OpenVA(vaArgs = settingsInterVA,
                         pipelineArgs = settingsPipeline,
                         odkID = settingsODK.odkID,
                         runDate = staticRunDate)

        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/previous_bc_export.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/another_bc_export.csv",
                    dirODK + "/odkBCExportNew.csv")

        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()
        rOpenVA.getCOD()

        self.assertTrue(os.path.isfile(rOutFile))
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

    def test_3_getCOD_interva_exception(self):
        """getCOD() should raise an exception with problematic interva R script."""
        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("InterVA", "InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        settingsInterVA = self.xferDB.configOpenVA(self.conn,
                                                   "InterVA",
                                                   settingsPipeline.workingDirectory)
        self.conn.rollback()
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                        strftime("%Y_%m_%d_%H:%M:%S")
        rOutFile = os.path.join(dirOpenVA, staticRunDate,
                                "Rscript_" + staticRunDate + ".Rout")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        rOpenVA = OpenVA(vaArgs = settingsInterVA,
                         pipelineArgs = settingsPipeline,
                         odkID = "this should raise an exception",
                         runDate = staticRunDate)

        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/previous_bc_export.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/another_bc_export.csv",
                    dirODK + "/odkBCExportNew.csv")

        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()

        self.assertRaises(OpenVAError, rOpenVA.getCOD)
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

    def test_3_getCOD_smartva(self):
        """Check that getCOD() executes smartva cli"""
        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("SmartVA", "SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        settingsSmartVA = self.xferDB.configOpenVA(self.conn,
                                                   "SmartVA",
                                                   settingsPipeline.workingDirectory)
        self.conn.rollback()
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                        strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/odkExport_phmrc-1.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/odkExport_phmrc-2.csv",
                    dirODK + "/odkBCExportNew.csv")

        cliSmartVA = OpenVA(vaArgs = settingsSmartVA,
                            pipelineArgs = settingsPipeline,
                            odkID = settingsODK.odkID,
                            runDate = staticRunDate)

        zeroRecords = cliSmartVA.copyVA()
        completed = cliSmartVA.getCOD()
        svaOut = os.path.join(
            dirOpenVA,
            staticRunDate,
            "1-individual-cause-of-death/individual-cause-of-death.csv"
        )

        self.assertTrue(os.path.isfile(svaOut))
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

    def test_3_getCOD_smartva_exception(self):
        """getCOD() should raise an exception with faulty args for smartva cli"""
        c = self.conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("SmartVA", "SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1")
        c.execute(sql, par)
        settingsPipeline = self.xferDB.configPipeline(self.conn)
        settingsODK = self.xferDB.configODK(self.conn)
        self.conn.rollback()
        ntSmartVA = collections.namedtuple("ntSmartVA",
                                           ["SmartVA_country",
                                            "SmartVA_hiv",
                                            "SmartVA_malaria",
                                            "SmartVA_hce",
                                            "SmartVA_freetext",
                                            "SmartVA_figures",
                                            "SmartVA_language"]
        )
        settingsSmartVA = ntSmartVA("Unknown",
                                    "Wrong",
                                    "Wrong",
                                    "Wrong",
                                    "Wrong",
                                    "Wrong",
                                    "Wrong")
        dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
        dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
        staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                        strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )
        if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
            os.remove(dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
            os.remove(dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/previous_bc_export.csv",
                    dirODK + "/odkBCExportPrev.csv")
        shutil.copy(dirODK + "/another_bc_export.csv",
                    dirODK + "/odkBCExportNew.csv")

        cliSmartVA = OpenVA(vaArgs = settingsSmartVA,
                            pipelineArgs = settingsPipeline,
                            odkID = settingsODK.odkID,
                            runDate = staticRunDate)

        zeroRecords = cliSmartVA.copyVA()

        self.assertRaises(SmartVAError, cliSmartVA.getCOD)
        shutil.rmtree(
            os.path.join(dirOpenVA, staticRunDate),
            ignore_errors = True
        )

    # def test_3_rmODKExport_odkExportNew(self):
    #     """Check that rmODKExport clears ODK Export after successful run"""
    #     c = self.conn.cursor()
    #     sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
    #     par = ("InterVA", "InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
    #     c.execute(sql, par)
    #     settingsPipeline = self.xferDB.configPipeline(self.conn)
    #     settingsODK = self.xferDB.configODK(self.conn)
    #     settingsInterVA = self.xferDB.configOpenVA(self.conn,
    #                                                "InterVA",
    #                                                settingsPipeline.workingDirectory)
    #     self.conn.rollback()
    #     dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
    #     dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
    #     staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
    #                     strftime("%Y_%m_%d_%H:%M:%S")
    #     rOutFile = os.path.join(dirOpenVA, staticRunDate,
    #                             "Rscript_" + staticRunDate + ".Rout")

    #     rOpenVA = OpenVA(vaArgs = settingsInterVA,
    #                      pipelineArgs = settingsPipeline,
    #                      odkID = settingsODK.odkID,
    #                      runDate = staticRunDate)

    #     if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
    #         os.remove(dirODK + "/odkBCExportNew.csv")
    #     if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
    #         os.remove(dirODK + "/odkBCExportPrev.csv")
    #     shutil.copy(dirODK + "/previous_bc_export.csv",
    #                 dirODK + "/odkBCExportPrev.csv")
    #     shutil.copy(dirODK + "/another_bc_export.csv",
    #                 dirODK + "/odkBCExportNew.csv")

    #     zeroRecords = rOpenVA.copyVA()
    #     rOpenVA.rScript()
    #     rOpenVA.getCOD()
    #     rOpenVA.rmODKExport()

    #     self.assertFalse(os.path.isfile(dirODK + "/odkBCExportNew.csv"))
    #     shutil.rmtree(
    #         os.path.join(dirOpenVA, staticRunDate),
    #         ignore_errors = True
    #     )

    # def test_3_rmODKExport_odkExportPrev(self):
    #     """Check that rmODKExport clears ODK Export after successful run"""
    #     c = self.conn.cursor()
    #     sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
    #     par = ("InterVA", "InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
    #     c.execute(sql, par)
    #     settingsPipeline = self.xferDB.configPipeline(self.conn)
    #     settingsODK = self.xferDB.configODK(self.conn)
    #     settingsInterVA = self.xferDB.configOpenVA(self.conn,
    #                                                "InterVA",
    #                                                settingsPipeline.workingDirectory)
    #     self.conn.rollback()
    #     dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
    #     dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
    #     staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
    #                     strftime("%Y_%m_%d_%H:%M:%S")
    #     rOutFile = os.path.join(dirOpenVA, staticRunDate,
    #                             "Rscript_" + staticRunDate + ".Rout")

    #     rOpenVA = OpenVA(vaArgs = settingsInterVA,
    #                      pipelineArgs = settingsPipeline,
    #                      odkID = settingsODK.odkID,
    #                      runDate = staticRunDate)

    #     if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
    #         os.remove(dirODK + "/odkBCExportNew.csv")
    #     if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
    #         os.remove(dirODK + "/odkBCExportPrev.csv")
    #     shutil.copy(dirODK + "/previous_bc_export.csv",
    #                 dirODK + "/odkBCExportPrev.csv")
    #     shutil.copy(dirODK + "/another_bc_export.csv",
    #                 dirODK + "/odkBCExportNew.csv")

    #     zeroRecords = rOpenVA.copyVA()
    #     rOpenVA.rScript()
    #     rOpenVA.getCOD()
    #     rOpenVA.rmODKExport()

    #     self.assertFalse(os.path.isfile(dirODK + "/odkBCExportPrev.csv"))
    #     shutil.rmtree(
    #         os.path.join(dirOpenVA, staticRunDate),
    #         ignore_errors = True
    #     )

    # def test_3_rmODKExport_raiseException(self):
    #     """Check that rmODKExport raises exception on unsuccessful run"""
    #     c = self.conn.cursor()
    #     sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
    #     par = ("InterVA", "InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1")
    #     c.execute(sql, par)
    #     settingsPipeline = self.xferDB.configPipeline(self.conn)
    #     settingsODK = self.xferDB.configODK(self.conn)
    #     settingsInterVA = self.xferDB.configOpenVA(self.conn,
    #                                                "InterVA",
    #                                                settingsPipeline.workingDirectory)
    #     self.conn.rollback()
    #     dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
    #     dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
    #     staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
    #                     strftime("%Y_%m_%d_%H:%M:%S")
    #     rOutFile = os.path.join(dirOpenVA, staticRunDate,
    #                             "Rscript_" + staticRunDate + ".Rout")

    #     rOpenVA = OpenVA(vaArgs = settingsInterVA,
    #                      pipelineArgs = settingsPipeline,
    #                      odkID = settingsODK.odkID,
    #                      runDate = staticRunDate)

    #     if os.path.isfile(dirODK + "/odkBCExportNew.csv"):
    #         os.remove(dirODK + "/odkBCExportNew.csv")
    #     if os.path.isfile(dirODK + "/odkBCExportPrev.csv"):
    #         os.remove(dirODK + "/odkBCExportPrev.csv")
    #     shutil.copy(dirODK + "/previous_bc_export.csv",
    #                 dirODK + "/odkBCExportPrev.csv")
    #     shutil.copy(dirODK + "/another_bc_export.csv",
    #                 dirODK + "/odkBCExportNew.csv")

    #     zeroRecords = rOpenVA.copyVA()
    #     self.assertRaises(OpenVAError, rOpenVA.rmODKExport)


if __name__ == "__main__":
    unittest.main()
