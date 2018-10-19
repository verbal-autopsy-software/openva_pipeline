#------------------------------------------------------------------------------#
# test_pipeline.py
#------------------------------------------------------------------------------#

import datetime
import subprocess
import shutil
import os
import unittest
import collections
import pandas as pd
from pysqlcipher3 import dbapi2 as sqlcipher

from context import dhis
from dhis import DHIS
from dhis import API
from dhis import VerbalAutopsyEvent
from context import transferDB
from transferDB import TransferDB
from pipeline import Pipeline

os.chdir(os.path.abspath(os.path.dirname(__file__)))

class Check_Pipeline_config(unittest.TestCase):
    """Check config method."""

    dbFileName = "Pipeline.db"
    dbDirectory = "."
    dbKey = "enilepiP"
    useDHIS = True
    pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
    settings = pl.config()
    settingsPipeline = settings["pipeline"]
    settingsODK = settings["odk"]
    settingsOpenVA = settings["openVA"]
    settingsDHIS = settings["dhis"][0]

    def test_config_pipeline_algorithmMetadataCode(self):
        """Test config method configuration of pipeline:
        settingsPipeline.algorithmMetadataCode."""
        self.assertEqual(self.settingsPipeline.algorithmMetadataCode,
            "InSilicoVA|1.1.4|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1")

    def test_config_pipeline_codSource(self):
        """Test config method configuration of pipeline:
        settingsPipeline.codSource."""
        self.assertEqual(self.settingsPipeline.codSource, "WHO")

    def test_config_pipeline_algorithm(self):
        """Test config method configuration of pipeline:
        settingsPipeline.algorithm."""
        self.assertEqual(self.settingsPipeline.algorithm, "InSilicoVA")

    def test_config_pipeline_workingDirecotry(self):
        """Test config method configuration of pipeline:
        settingsPipeline.workingDirectory."""
        self.assertEqual(self.settingsPipeline.workingDirectory, ".")

    def test_config_odk_odkID(self):
        """Test config method configuration of pipeline:
        settingsODK.odkID."""
        self.assertEqual(self.settingsODK.odkID, None)

    def test_config_odk_odkURL(self):
        """Test config method configuration of pipeline:
        settingsODK.odkURL."""
        self.assertEqual(self.settingsODK.odkURL,
                         "https://odk.swisstph.ch/ODKAggregateOpenVa")

    def test_config_odk_odkUser(self):
        """Test config method configuration of pipeline:
        settingsODK.odkUser."""
        self.assertEqual(self.settingsODK.odkUser, "odk_openva")

    def test_config_odk_odkPassword(self):
        """Test config method configuration of pipeline:
        settingsODK.odkPassword."""
        self.assertEqual(self.settingsODK.odkPassword, "openVA2018")

    def test_config_odk_odkFormID(self):
        """Test config method configuration of pipeline:
        settingsODK.odkFormID."""
        self.assertEqual(self.settingsODK.odkFormID,
                         "va_who_2016_11_03_v1_4_1")

    def test_config_odk_odkLastRun(self):
        """Test config method configuration of pipeline:
        settingsODK.odkLastRun."""
        self.assertEqual(self.settingsODK.odkLastRun, "1900-01-01_00:00:01")

    def test_config_openva_InSilicoVA_data_type(self):
        """Test config method configuration of pipeline:
        settingsOpenVA.InSilicoVA_data_type."""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_data_type, "WHO2012")

    def test_config_openva_InSilicoVA_Nsim(self):
        """Test config method configuration of pipeline:
        settingsOpenVA.InSilicoVA_Nsim."""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_Nsim, "4000")

    def test_config_dhis_dhisURL(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisURL."""
        self.assertEqual(self.settingsDHIS.dhisURL, "https://va30se.swisstph-mis.ch")

    def test_config_dhis_dhisUser(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisUser."""
        self.assertEqual(self.settingsDHIS.dhisUser, "va-demo")

    def test_config_dhis_dhisPassword(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisPassword."""
        self.assertEqual(self.settingsDHIS.dhisPassword, "VerbalAutopsy99!")

    def test_config_dhis_dhisOrgUnit(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisOrgUnit."""
        self.assertEqual(self.settingsDHIS.dhisOrgUnit, "SCVeBskgiK6")

class Check_Pipeline_runODK(unittest.TestCase):
    """Check runODK method."""

    def test_runODK_1(self):
        """Test runODK method copies previous file."""

        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]

        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportPrev.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_runODK_2(self):
        """Test runODK method downloads file."""

        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]

        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportPrev.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_runODK_3(self):
        """Check mergeToPrevExport() includes all VA records from ODK BC export files."""

        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]

        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)

        hasAll = True
        with open("ODKFiles/odkBCExportPrev.csv") as fCombined:
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
        os.remove("ODKFiles/odkBCExportPrev.csv")
        

    def test_runODK_4(self):
        """Check successful run with valid parameters."""

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors = True)

        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]

        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)
        self.assertEqual(0, odkBC.returncode)

    def test_runODK_5(self):
        """Check for exported CSV file."""

        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")

        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]

        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportNew.csv"))

class Check_Pipeline_runOpenVA(unittest.TestCase):
    """Check runOpenVA method sets up files correctly"""

    dbFileName = "Pipeline.db"
    copy_dbFileName = "copy_Pipeline.db"
    dbDirectory = "."
    dbKey = "enilepiP"
    useDHIS = True
    dirODK = "ODKFiles"
    dirOpenVA = "OpenVAFiles"

    def test_runOpenVA_1(self):
        """Check that runOpenVA() brings in new file."""
        if os.path.isfile(self.dirOpenVA + "/openVA_input.csv"):
            os.remove(self.dirOpenVA + "/openVA_input.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")

        pl = Pipeline(self.dbFileName, self.dbDirectory, self.dbKey, self.useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]
        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)
        self.assertTrue(
            os.path.isfile("OpenVAFiles/openVA_input.csv")
        )

    def test_runOpenVA_2(self):
        """Check that runOpenVA() includes all records."""
        if os.path.isfile(self.dirOpenVA + "/openVA_input.csv"):
            os.remove(self.dirOpenVA + "/openVA_input.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportNew.csv"):
            os.remove(self.dirODK + "/odkBCExportNew.csv")
        if os.path.isfile(self.dirODK + "/odkBCExportPrev.csv"):
            os.remove(self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/previous_bc_export.csv",
                    self.dirODK + "/odkBCExportPrev.csv")
        shutil.copy(self.dirODK + "/another_bc_export.csv",
                    self.dirODK + "/odkBCExportNew.csv")
        pl = Pipeline(self.dbFileName, self.dbDirectory, self.dbKey, self.useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]
        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)
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
        shutil.rmtree("OpenVAFiles/" + pl.pipelineRunDate)

    def test_runOpenVA_3(self):
        """Check that runOpenVA() returns zeroRecords == True."""

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

        plZero = Pipeline(self.dbFileName,
                          self.dbDirectory,
                          self.dbKey,
                          self.useDHIS)
        settings = plZero.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]
        rOut = plZero.runOpenVA(settingsOpenVA,
                                settingsPipeline,
                                settingsODK.odkID,
                                plZero.pipelineRunDate)

        self.assertTrue(rOut["zeroRecords"])
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")

    def test_runOpenVA_4(self):
        """Check that runOpenVA() returns zeroRecords = FALSE"""

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

        plZero = Pipeline(self.dbFileName,
                          self.dbDirectory,
                          self.dbKey,
                          self.useDHIS)
        settings = plZero.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]
        rOut = plZero.runOpenVA(settingsOpenVA,
                                settingsPipeline,
                                settingsODK.odkID,
                                plZero.pipelineRunDate)
        self.assertFalse(rOut["zeroRecords"])
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")
        os.remove(self.dirOpenVA + "/openVA_input.csv")
        shutil.rmtree("OpenVAFiles/" + plZero.pipelineRunDate)
      
    def test_runOpenVA_5(self):
        """Check that runOpenVA() doesn't create new file if returns zeroRecords == True."""

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

        plZero = Pipeline(self.dbFileName,
                          self.dbDirectory,
                          self.dbKey,
                          self.useDHIS)
        settings = plZero.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]
        rOut = plZero.runOpenVA(settingsOpenVA,
                                settingsPipeline,
                                settingsODK.odkID,
                                plZero.pipelineRunDate)

        self.assertFalse(
            os.path.isfile(self.dirOpenVA + "/openVA_input.csv")
        )
        os.remove(self.dirODK + "/odkBCExportPrev.csv")
        os.remove(self.dirODK + "/odkBCExportNew.csv")

class Check_Pipeline_runOpenVA_InSilicoVA(unittest.TestCase):
    """Check runOpenVA method runs InSilicoVA"""

    dbFileName = "copy_Pipeline.db"
    dbKey = "enilepiP"
    dbDirectory = "."
    nowDate = datetime.datetime.now()
    pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")

    xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()

    c = conn.cursor()
    sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
    par = ("InSilicoVA", "InSilicoVA|1.1.4|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1");
    c.execute(sql, par)
    sql = "UPDATE InSilicoVA_Conf SET data_type = ?"
    par = ("WHO2012",);
    c.execute(sql, par)
    conn.commit()
    conn.close()
    pl = Pipeline(dbFileName,
                  dbDirectory,
                  dbKey,
                  True)
    settings = pl.config()
    settingsPipeline = settings["pipeline"]
    settingsODK = settings["odk"]
    settingsOpenVA = settings["openVA"]
    settingsDHIS = settings["dhis"][0]

    dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
    dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
    shutil.rmtree(
        os.path.join(dirOpenVA, pl.pipelineRunDate),
        ignore_errors = True
    )
    if os.path.isfile("OpenVAFiles/recordStorage.csv"):
        os.remove("OpenVAFiles/recordStorage.csv")
        
    odkBC = pl.runODK(settingsODK,
                      settingsPipeline.workingDirectory)
    rOut = pl.runOpenVA(settingsOpenVA,
                        settingsPipeline,
                        settingsODK.odkID,
                        pl.pipelineRunDate)

    def test_runOpenVA_InSilico_1(self):
        """Check that runOpenVA() creates an R script for InSilicoVA."""
        rScriptFile = os.path.join(self.dirOpenVA,
                                   self.pl.pipelineRunDate,
                                   "Rscript_" + self.pl.pipelineRunDate + ".R")
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InSilico_2(self):
        """Check that runOpenVA() runs R script for InSilicoVA."""
        rScriptFile = os.path.join(self.dirOpenVA,
                                   self.pl.pipelineRunDate,
                                   "Rscript_" + self.pl.pipelineRunDate + ".Rout")
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InSilico_3(self):
        """Check that runOpenVA() creates resuls file for InSilicoVA script."""
        rScriptFile = os.path.join(self.dirOpenVA,
                                   self.pl.pipelineRunDate,
                                   "Rscript_" + self.pl.pipelineRunDate + ".R")
        self.assertTrue(os.path.isfile(rScriptFile))
        shutil.rmtree("OpenVAFiles/" + self.pl.pipelineRunDate)

class Check_Pipeline_runOpenVA_InterVA(unittest.TestCase):
    """Check runOpenVA method runs InterVA"""

    dbFileName = "copy_Pipeline.db"
    dbKey = "enilepiP"
    dbDirectory = "."
    nowDate = datetime.datetime.now()
    pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")

    xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()

    c = conn.cursor()
    sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
    par = ("InterVA", "InterVA4|4.04|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1")
    c.execute(sql, par)
    conn.commit()
    conn.close()
    pl = Pipeline(dbFileName,
                  dbDirectory,
                  dbKey,
                  True)
    settings = pl.config()
    settingsPipeline = settings["pipeline"]
    settingsODK = settings["odk"]
    settingsOpenVA = settings["openVA"]
    settingsDHIS = settings["dhis"][0]

    dirOpenVA = os.path.join(settingsPipeline.workingDirectory, "OpenVAFiles")
    dirODK = os.path.join(settingsPipeline.workingDirectory, "ODKFiles")
    shutil.rmtree(
        os.path.join(dirOpenVA, pl.pipelineRunDate),
        ignore_errors = True
    )
    if os.path.isfile("OpenVAFiles/recordStorage.csv"):
        os.remove("OpenVAFiles/recordStorage.csv")

    rOut = pl.runOpenVA(settingsOpenVA,
                        settingsPipeline,
                        settingsODK.odkID,
                        pl.pipelineRunDate)

    def test_runOpenVA_InterVA_1(self):
        """Check that runOpenVA() creates an R script for InterVA."""
        rScriptFile = os.path.join(self.dirOpenVA,
                                   self.pl.pipelineRunDate,
                                   "Rscript_" + self.pl.pipelineRunDate + ".R")
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InterVA_2(self):
        """Check that runOpenVA() runs R script for InterVA."""
        rScriptFile = os.path.join(self.dirOpenVA,
                                   self.pl.pipelineRunDate,
                                   "Rscript_" + self.pl.pipelineRunDate + ".Rout")
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InterVA_3(self):
        """Check that runOpenVA() creates resuls file for InterVA script."""
        rScriptFile = os.path.join(self.dirOpenVA,
                                   self.pl.pipelineRunDate,
                                   "Rscript_" + self.pl.pipelineRunDate + ".R")
        self.assertTrue(os.path.isfile(rScriptFile))
        shutil.rmtree("OpenVAFiles/" + self.pl.pipelineRunDate)

class Check_runOpenVA_SmartVA(unittest.TestCase):
    """Check runOpenVA method runs SmartVA"""

    def test_runOpenVA_SmartVA_1(self):
        """Check that runOpenVA() executes smartva cli"""
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/odkExport_phmrc-1.csv",
                    "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/odkExport_phmrc-2.csv",
                    "ODKFiles/odkBCExportNew.csv")

        dbFileName = "copy_Pipeline.db"
        dbKey = "enilepiP"
        dbDirectory = "."
        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")

        xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?"
        par = ("SmartVA", "SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1")
        c.execute(sql, par)
        conn.commit()
        conn.close()
        pl = Pipeline(dbFileName,
                      dbDirectory,
                      dbKey,
                      True)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"][0]

        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)

        svaOut = os.path.join(
            "OpenVAFiles",
            pl.pipelineRunDate,
            "1-individual-cause-of-death/individual-cause-of-death.csv"
        )

        self.assertTrue(os.path.isfile(svaOut))
        shutil.rmtree(
            os.path.join("OpenVAFiles", pl.pipelineRunDate),
            ignore_errors = True
        )
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")

class Check_Pipeline_runDHIS(unittest.TestCase):
    """Check runDHIS method"""

    def test_runDHIS_1_vaProgramUID(self):
        """Verify VA program is installed."""
        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory,
                      dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"]
        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)
        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)
        pipelineDHIS = pl.runDHIS(settingsDHIS,
                                  settingsPipeline.workingDirectory)
        self.assertEqual(pipelineDHIS["vaProgramUID"], "sv91bCroFFx")
        shutil.rmtree("OpenVAFiles/" + pl.pipelineRunDate)
        shutil.rmtree("DHIS/blobs/")
        

    def test_runDHIS_2_postVA(self):
        """Post VA records to DHIS2."""
        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory,
                      dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"]
        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)
        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)
        pipelineDHIS = pl.runDHIS(settingsDHIS,
                                  settingsPipeline.workingDirectory)
        postLog = pipelineDHIS["postLog"]
        checkLog = 'importSummaries' in postLog["response"].keys()
        self.assertTrue(checkLog)
        shutil.rmtree("OpenVAFiles/" + pl.pipelineRunDate)
        shutil.rmtree("DHIS/blobs/")

    def test_runDHIS_3_verifyPost(self):
        """Verify VA records got posted to DHIS2."""
        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory,
                      dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"]
        odkBC = pl.runODK(settingsODK,
                          settingsPipeline.workingDirectory)
        rOut = pl.runOpenVA(settingsOpenVA,
                            settingsPipeline,
                            settingsODK.odkID,
                            pl.pipelineRunDate)
        pipelineDHIS = pl.runDHIS(settingsDHIS,
                                  settingsPipeline.workingDirectory)
        dfNewStorage = pd.read_csv("OpenVAFiles/newStorage.csv")
        nPushed = sum(dfNewStorage["pipelineOutcome"] == "Pushed to DHIS2")
        self.assertEqual(nPushed, pipelineDHIS["nPostedRecords"])
        shutil.rmtree("OpenVAFiles/" + pl.pipelineRunDate)
        shutil.rmtree("DHIS/blobs")

class Check_Pipeline_depositResults(unittest.TestCase):

    def test_storeVA(self):
        """Check that depositResults() stores VA records in Transfer DB."""
        shutil.copy("OpenVAFiles/sample_newStorage.csv",
                    "OpenVAFiles/newStorage.csv")

        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")
        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        c.execute("DELETE FROM VA_Storage;")
        conn.commit()
        conn.close()

        pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
        settings = pl.config()
        settingsPipeline = settings["pipeline"]
        settingsODK = settings["odk"]
        settingsOpenVA = settings["openVA"]
        settingsDHIS = settings["dhis"]

        pl.storeResultsDB()
        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sql = "SELECT id FROM VA_Storage"
        c.execute(sql)
        vaIDs = c.fetchall()
        conn.close()
        vaIDsList = [j for i in vaIDs for j in i]
        s1 = set(vaIDsList)
        dfNewStorage = pd.read_csv("OpenVAFiles/newStorage.csv")
        dfNewStorageID = dfNewStorage["odkMetaInstanceID"]
        s2 = set(dfNewStorageID)
        self.assertTrue(s2.issubset(s1))

class Check_Pipeline_closePipeline(unittest.TestCase):

    def test_DHIS_cleanPipeline_rmFiles(self):
        """Test file removal."""
        if not os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            shutil.copy("ODKFiles/previous_bc_export.csv",
                        "ODKFiles/odkBCExportPrev.csv")
        if not os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            shutil.copy("ODKFiles/another_bc_export.csv",
                        "ODKFiles/odkBCExportNew.csv")

        if not os.path.isfile("OpenVAFiles/openVA_input.csv"):
            shutil.copy("OpenVAFiles/sample_openVA_input.csv",
                        "OpenVAFiles/openVA_input.csv")
        if not os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            shutil.copy("OpenVAFiles/sampleEAV.csv",
                        "OpenVAFiles/entityAttributeValue.csv")
        if not os.path.isfile("OpenVAFiles/recordStorage.csv"):
            shutil.copy("OpenVAFiles/sample_recordStorage.csv",
                        "OpenVAFiles/recordStorage.csv")
        if not os.path.isfile("OpenVAFiles/newStorage.csv"):
            shutil.copy("OpenVAFiles/sample_newStorage.csv",
                        "OpenVAFiles/newStorage.csv")

        os.makedirs("DHIS/blobs/", exist_ok = True)
        shutil.copy("OpenVAFiles/sample_newStorage.csv",
                    "DHIS/blobs/001-002-003.db")

        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")
        pl = Pipeline(dbFileName, dbDirectory, dbKey, useDHIS)
        pl.closePipeline()
        fileExist = False
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            fileExist = True
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            fileExist = True
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            fileExist = True
        if os.path.isfile("OpenVAFiles/openVA_input.csv"):
            fileExist = True
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            fileExist = True
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            fileExist = True
        if os.path.isfile("OpenVAFiles/newStorage.csv"):
            fileExist = True
        if os.path.isfile("DHIS/blobs/001-002-003.db"):
            fileExist = True
        self.assertFalse(fileExist)

    def test_DHIS_cleanPipeline_odkLastRun(self):
        """Test update of ODK_Conf.odkLastRun."""

        os.makedirs("DHIS/blobs/", exist_ok = True)
        dbFileName = "Pipeline.db"
        dbDirectory = "."
        dbKey = "enilepiP"
        useDHIS = True
        pl = Pipeline(dbFileName, dbDirectory,
                      dbKey, useDHIS)
        pl.closePipeline()

        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime("%Y-%m-%d_%H:%M:%S")
        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        c.execute("SELECT odkLastRun FROM ODK_Conf;")
        sqlQuery = c.fetchone()
        results = [i for i in sqlQuery]
        self.assertEqual(results[0], pipelineRunDate)
        c.execute("UPDATE ODK_Conf SET odkLastRun = ?;")
        conn.commit()
        conn.close()

if __name__ == "__main__":
    unittest.main()
