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

    def test_config_runODK_1(self):
        """Test runODK method copies previous file."""

        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportNew.csv")
        self.pl.runODK()
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportPrev.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_config_runODK_2(self):
        """Test runODK method downloads file."""

        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        self.pl.runODK()
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportPrev.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_config_runODK_3(self):
        """Check mergeToPrevExport() removes new ODK BC export files."""
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        self.pl.runODK()
        self.assertFalse(os.path.isfile("ODKFiles/odkBCExportNew.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_config_runODK_4(self):
        """Check mergeToPrevExport() includes all VA records from ODK BC export files."""
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        self.pl.runODK()

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

    def test_config_runODK_5(self):
        """Check successful run with valid parameters."""

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors = True)

        x.returncode = self.pl.runODK()
        self.assertEqual(0, x.returncode)

    def test_config_runODK_6(self):
        """Check for exported CSV file."""
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportNew.csv"))

if __name__ == "__main__":
    unittest.main()
