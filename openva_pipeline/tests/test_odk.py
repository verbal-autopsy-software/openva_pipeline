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
    bcExportDir = "Temp"
    bcStorageDir = "Temp"
    # bcFileName = "bc_export_" + \
    #   datetime.date.today().strftime("%Y_%m_%d") + ".csv"

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
        """Check mergePrevExport() moves odkBCExportNew.csv"""
        if os.path.isfile("Temp/odkBCExportNew.csv"):
            os.remove("Temp/odkBCExportNew.csv")
        if os.path.isfile("Temp/odkBCExportPrev.csv"):
            os.remove("Temp/odkBCExportPrev.csv")
        shutil.copy("Temp/previous_bc_export.csv", "Temp/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergePrevExport()
        self.assertTrue(os.path.isfile("Temp/odkBCExportPrev.csv"))
        os.remove("Temp/odkBCExportPrev.csv")

    def test_ODK_briefcase_2(self):
        """Check mergePrevExport() merges ODK BC export files."""
        if os.path.isfile("Temp/odkBCExportNew.csv"):
            os.remove("Temp/odkBCExportNew.csv")
        if os.path.isfile("Temp/odkBCExportPrev.csv"):
            os.remove("Temp/odkBCExportPrev.csv")
        shutil.copy("Temp/previous_bc_export.csv", "Temp/odkBCExportPrev.csv")
        shutil.copy("Temp/another_bc_export.csv", "Temp/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergePrevExport()
        self.assertTrue(os.path.isfile("Temp/odkBCExportPrev.csv"))
        os.remove("Temp/odkBCExportPrev.csv")

    def test_ODK_briefcase_3(self):
        """Check mergePrevExport() merges ODK BC export files."""
        if os.path.isfile("Temp/odkBCExportNew.csv"):
            os.remove("Temp/odkBCExportNew.csv")
        if os.path.isfile("Temp/odkBCExportPrev.csv"):
            os.remove("Temp/odkBCExportPrev.csv")
        shutil.copy("Temp/previous_bc_export.csv", "Temp/odkBCExportPrev.csv")
        shutil.copy("Temp/another_bc_export.csv", "Temp/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergePrevExport()
        self.assertFalse(os.path.isfile("Temp/odkBCExportNew.csv"))
        os.remove("Temp/odkBCExportPrev.csv")

    def test_ODK_briefcase_4(self):
        """Check mergePrevExport() includes al VA records from ODK BC export files."""
        if os.path.isfile("Temp/odkBCExportNew.csv"):
            os.remove("Temp/odkBCExportNew.csv")
        if os.path.isfile("Temp/odkBCExportPrev.csv"):
            os.remove("Temp/odkBCExportPrev.csv")
        shutil.copy("Temp/previous_bc_export.csv", "Temp/odkBCExportPrev.csv")
        shutil.copy("Temp/another_bc_export.csv", "Temp/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK,
                              self.bcExportDir,
                              self.bcStorageDir)
        pipelineODK.mergePrevExport()

        hasAll = True
        with open("Temp/odkBCExportPrev.csv") as fCombined:
            fCombinedLines = fCombined.readlines()
        with open("Temp/previous_bc_export.csv") as fPrevious:
            fPreviousLines = fPrevious.readlines()
        with open("Temp/another_bc_export.csv") as fAnother:
            fAnotherLines = fAnother.readlines()
        for line in fPreviousLines:
            if line not in fCombinedLines:
                hasAll = False
        for line in fAnotherLines:
            if line not in fCombinedLines:
                hasAll = False
        self.assertTrue(hasAll)
        os.remove("Temp/odkBCExportPrev.csv")

    # def test_ODK_briefcase_4(self):
    #     """Check successful run with valid parameters."""

    #     shutil.rmtree("Temp/ODK Briefcase Storage/")

    #     pipelineODK = odk.ODK(self.settingsODK,
    #                           self.bcExportDir,
    #                           self.bcStorageDir)
    #     odkBC = pipelineODK.briefcase()

    #     self.assertEqual(0, odkBC.returncode)

    # def test_ODK_briefcase_3(self):
    #     """Check for exported CSV file."""
    #     exportFile = self.bcExportDir + "/" + self.bcFileName
    #     self.assertTrue(os.path.isfile(exportFile))

    # def test_ODK_briefcase_4(self):
    #     """Check method for merging export files."""
    #     copy over new file that you created, then run prepareExport()

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
    bcExportDir = "Temp"
    bcStorageDir = "Temp"
    # bcFileName = "bc_export_" + \
    #   datetime.date.today().strftime("%Y_%m_%d") + ".csv"

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
