#------------------------------------------------------------------------------#
# test_odk.py
#------------------------------------------------------------------------------#

import datetime
import subprocess
import shutil
import os
import unittest
import collections

import context
from openva_pipeline import odk

os.chdir(os.path.abspath(os.path.dirname(__file__)))

class CompleteFreshRun(unittest.TestCase):
    """Check successful completion from blank slate."""

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

    def setUp(self):
        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors = True)

        self.pipelineODK = odk.ODK(self.settingsODK, ".")
        self.odkBC = self.pipelineODK.briefcase()

    def test_briefcase_returncode(self):
        """Check successful run with valid parameters."""
        self.assertEqual(0, self.odkBC.returncode)

    def test_briefcase_creates_file_odkBCExportNew(self):
        """Check for exported CSV file."""
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportNew.csv"))

    def test_mergeToPrevExport(self):
        """Check mergeToPrevExport() moves odkBCExportNew.csv to odkBCExportPrev.csv"""
        self.pipelineODK.mergeToPrevExport()
        self.assertTrue(os.path.isfile("ODKFiles/odkBCExportPrev.csv"))


class ProperMergeWithExistingExports(unittest.TestCase):
    """Check that unique VA records get preserved with new & exports."""

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
    def setUp(self):
        if os.path.isfile("ODKFiles/odkBCExportNew.csv"):
            os.remove("ODKFiles/odkBCExportNew.csv")
        if os.path.isfile("ODKFiles/odkBCExportPrev.csv"):
            os.remove("ODKFiles/odkBCExportPrev.csv")

        shutil.copy("ODKFiles/previous_bc_export.csv", "ODKFiles/odkBCExportPrev.csv")
        shutil.copy("ODKFiles/another_bc_export.csv", "ODKFiles/odkBCExportNew.csv")

        pipelineODK = odk.ODK(self.settingsODK, ".")
        pipelineODK.mergeToPrevExport()

    def test_unique_records_are_preserved(self):
        """Check mergeToPrevExport() includes all VA records from ODK BC export files."""

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

    def test_mergeToPrevExport(self):
        """Check mergeToPrevExport() moves odkBCExportNew.csv to odkBCExportPrev.csv"""
        self.assertFalse(os.path.isfile("ODKFiles/odkBCExportNew.csv"))


class DownloadBriefcaseTests(unittest.TestCase):
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

    def setUp(self):
        if os.path.isfile("ODK-Briefcase-v1.12.2.jar"):
            os.remove("ODK-Briefcase-v1.12.2.jar")
        pipelineODK = odk.ODK(self.settingsODK, ".")
        pipelineODK.downloadBriefcase()

    def test_downloadBriefcase(self):
        """Check downloadBriefcase()"""
        self.assertTrue(os.path.isfile("ODK-Briefcase-v1.12.2.jar"))


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
    badSettingsODK = ntODK(odkID,
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

        shutil.rmtree("ODKFiles/ODK Briefcase Storage", ignore_errors = True)
        pipelineODK = odk.ODK(self.badSettingsODK, ".")
        self.assertRaises(odk.ODKError, pipelineODK.briefcase)

if __name__ == "__main__":
    unittest.main()
