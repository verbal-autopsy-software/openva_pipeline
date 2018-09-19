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
    odkLastRunResult = "0"
    bcExportDir = "Temp"
    bcStorageDir = "Temp"
    bcFileName = "bc_export_" + \
      datetime.date.today().strftime("%Y_%m_%d") + ".csv"

    def test_1_ODK_connect(self):
        """Check successful run with valid parameters."""

        shutil.rmtree("Temp/ODK Briefcase Storage/")

        x = odk.ODK(odkID = self.odkID,
                    odkURL = self.odkURL,
                    odkUser = self.odkUser,
                    odkPassword = self.odkPassword,
                    odkFormID = self.odkFormID,
                    odkLastRun = self.odkLastRun,
                    odkLastRunDate = self.odkLastRunDate,
                    odkLastRunDatePrev = self.odkLastRunDatePrev,
                    odkLastRunResult = self.odkLastRunResult,
                    bcExportDir = self.bcExportDir,
                    bcStorageDir = self.bcStorageDir,
                    bcFileName = self.bcFileName)
        odkBundle = x.connect()

        self.assertEqual(0, odkBundle.returncode)

    def test_2_ODK_connect(self):
        """Check for exported CSV file."""
        exportFile = self.bcExportDir + "/" + self.bcFileName
        self.assertTrue(os.path.isfile(exportFile))


class InvalidConnection(unittest.TestCase):
    """Check that proper execptions are raised.""" ## HERE -- need to raise pipeline-specific exceptions

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
    odkLastRunResult = "0"
    bcExportDir = "Temp"
    bcStorageDir = "Temp"
    bcFileName = "bc_export_" + \
      datetime.date.today().strftime("%Y_%m_%d") + ".csv"

    def test_ODK_bad_odkID(self):
        """Check for error if odkID parameter is invalid."""

        x = odk.ODK(odkID = self.odkID,
                    odkURL = self.odkURL,
                    odkUser = "WRONG",
                    odkPassword = self.odkPassword,
                    odkFormID = self.odkFormID,
                    odkLastRun = self.odkLastRun,
                    odkLastRunDate = self.odkLastRunDate,
                    odkLastRunDatePrev = self.odkLastRunDatePrev,
                    odkLastRunResult = self.odkLastRunResult,
                    bcExportDir = self.bcExportDir,
                    bcStorageDir = self.bcStorageDir,
                    bcFileName = self.bcFileName)
        odkBundle = x.connect()

        self.assertEqual(1, odkBundle.returncode)


if __name__ == "__main__":
    unittest.main()

