#------------------------------------------------------------------------------#
# test_odk.py
#
# New tests:
#
# (1) make separate tests depending on version of ODK Briefcase?
#
#------------------------------------------------------------------------------#

import datetime
import subprocess
from context import odk
from odk import ODK
import shutil
import os
import unittest
import collections

os.chdir(os.path.abspath(os.path.dirname(__file__)))

class ValidConnection(unittest.TestCase):
    """Check the everything works as it should."""

    # Define valid parameters for SwissTPH ODK Aggregate Server.
    odkID = None
    odkURL = "https://odk.swisstph.ch/ODKAggregateOpenVa"
    odkUser = "odk_openva"
    odkPassword = "openVA2018"
    odkFormID = "va_who_2016_11_03_v1_4_1"
    odkLastRun = "1901-01-01_00:00:01"
    odkLastRunDate = datetime.datetime.strptime(
        odkLastRun, "%Y-%m-%d_%H:%M:%S").strftime("%Y/%m/%d")
    odkLastRunDatePrev = (
        datetime.datetime.strptime(odkLastRunDate, "%Y/%m/%d") -
        datetime.timedelta(days=1)
    ).strftime("%Y/%m/%d")
    odkLastRunResult = "fail"
    bcExportDir = "ODKFiles"
    bcStorageDir = "ODKFiles"

    ntODK = collections.namedtuple("ntODK",
                                   ["odkID",
                                    "odkURL",
                                    "odkUser",
                                    "odkPassword",
                                    "odkFormID",
                                    "odkLastRun",
                                    "odkLastRunResult",
                                    "odkLastRunDate",
                                    "odkLastRunDatePrev"]
        )
    settingsODK = ntODK(odkID,
                        odkURL,
                        odkUser,
                        odkPassword,
                        odkFormID,
                        odkLastRun,
                        odkLastRunResult,
                        odkLastRunDate,
                        odkLastRunDatePrev)

    def test_ODK_briefcase_1(self):
        """Check mergeToPrevExport() moves odkBCExportNew.csv"""
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergeToPrevExport()
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportPrev.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_ODK_briefcase_2(self):
        """Check mergePrevToExport() merges ODK BC export files."""
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergeToPrevExport()
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportPrev.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_ODK_briefcase_3(self):
        """Check mergeToPrevExport() removes new ODK BC export files."""
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergeToPrevExport()
        self.assertFalse(os.path.isfile("ODKFiles/odkBCExportNew.csv"))
        os.remove("ODKFiles/odkBCExportPrev.csv")

    def test_ODK_briefcase_4(self):
        """Check mergeToPrevExport() includes all VA records from ODK BC export files."""
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergeToPrevExport()

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

    def test_ODK_briefcase_5(self):
        """Check successful run with valid parameters."""

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors = True)

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        odkBC = pipelineODK.briefcase()

        self.assertEqual(0, odkBC.returncode)

    def test_ODK_briefcase_6(self):
        """Check for exported CSV file."""
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportNew.csv"))

class InvalidConnection(unittest.TestCase):
    """Check that proper execptions are raised."""

    # Define valid parameters for SwissTPH ODK Aggregate Server.
    odkID = None
    odkURL = "https://odk.swisstph.ch/ODKAggregateOpenVa"
    odkUser = "odk_openva"
    odkPassword = "openVA2018"
    odkFormID = "va_who_2016_11_03_v1_4_1"
    odkLastRun = "1901-01-01_00:00:01"
    odkLastRunDate = datetime.datetime.strptime(
        odkLastRun, "%Y-%m-%d_%H:%M:%S").strftime("%Y/%m/%d")
    odkLastRunDatePrev = (
        datetime.datetime.strptime(odkLastRunDate, "%Y/%m/%d") -
        datetime.timedelta(days=1)
    ).strftime("%Y/%m/%d")
    odkLastRunResult = "fail"
    bcExportDir = "ODKFiles"
    bcStorageDir = "ODKFiles"

    ntODK = collections.namedtuple("ntODK",
                                   ["odkID",
                                    "odkURL",
                                    "odkUser",
                                    "odkPassword",
                                    "odkFormID",
                                    "odkLastRun",
                                    "odkLastRunResult",
                                    "odkLastRunDate",
                                    "odkLastRunDatePrev"]
        )
    settingsODK = ntODK(odkID,
                        odkURL,
                        "WRONG",
                        odkPassword,
                        odkFormID,
                        odkLastRun,
                        odkLastRunResult,
                        odkLastRunDate,
                        odkLastRunDatePrev)

    def test_ODK_bad_odkID(self):
        """Check for error if odkID parameter is invalid."""

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        self.assertRaises(odk.ODKBriefcaseError, pipelineODK.briefcase)


if __name__ == "__main__":
    unittest.main()
