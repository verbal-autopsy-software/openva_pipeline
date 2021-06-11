from openva_pipeline import odk
from openva_pipeline.runPipeline import downloadBriefcase

import datetime
import shutil
import os
import glob
import unittest
import collections

from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
#import context

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class CompleteFreshRun(unittest.TestCase):
    """Check successful completion from blank slate."""

    @classmethod
    def setUpClass(cls):
        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors=True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')

        if not os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            downloadBriefcase()

        odkID = None
        odkURL = 'https://odk.swisstph.ch/ODKAggregateOpenVa'
        odkUser = 'odk_openva'
        odkPassword = 'openVA2018'
        odkFormID = 'va_who_v1_5_1'
        odkLastRun = '1901-01-01_00:00:01'
        odkLastRunDate = datetime.datetime.strptime(
            odkLastRun, '%Y-%m-%d_%H:%M:%S').strftime('%Y/%m/%d')
        odkLastRunDatePrev = (
            datetime.datetime.strptime(odkLastRunDate, '%Y/%m/%d') -
            datetime.timedelta(days=1)
        ).strftime('%Y/%m/%d')
        odkLastRunResult = 'fail'
        odkUseCentral = 'False'
        odkProjectNumber = '1'

        ntODK = collections.namedtuple('ntODK',
                                       ['odkID',
                                        'odkURL',
                                        'odkUser',
                                        'odkPassword',
                                        'odkFormID',
                                        'odkLastRun',
                                        'odkLastRunResult',
                                        'odkLastRunDate',
                                        'odkLastRunDatePrev',
                                        'odkUseCentral',
                                        'odkProjectNumber'])
        settingsODK = ntODK(odkID,
                            odkURL,
                            odkUser,
                            odkPassword,
                            odkFormID,
                            odkLastRun,
                            odkLastRunResult,
                            odkLastRunDate,
                            odkLastRunDatePrev,
                            odkUseCentral,
                            odkProjectNumber)

        cls.pipelineODK = odk.ODK(settingsODK, '.')
        if not os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            downloadBriefcase()
        cls.odkBC = cls.pipelineODK.briefcase()

    def test_briefcase_returncode(self):
        """Check successful run with valid parameters."""

        self.assertEqual(0, self.odkBC.returncode)

    def test_briefcase_creates_file_odkBCExportNew(self):
        """Check for exported CSV file."""

        self.assertTrue(os.path.isfile('ODKFiles/odkBCExportNew.csv'))

    def test_mergeToPrevExport(self):
        """Check mergeToPrevExport() moves odkBCExportNew.csv to odkBCExportPrev.csv"""

        self.pipelineODK.mergeToPrevExport()
        self.assertTrue(os.path.isfile('ODKFiles/odkBCExportPrev.csv'))

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors=True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        for bcLog in glob.glob('briefcase*.log'):
            os.remove('./' + bcLog)


class ProperMergeWithExistingExports(unittest.TestCase):
    """Check that unique VA records get preserved with new & exports."""

    @classmethod
    def setUpClass(cls):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors=True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')

        if not os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            downloadBriefcase()

        odkID = None
        odkURL = 'https://odk.swisstph.ch/ODKAggregateOpenVa'
        odkUser = 'odk_openva'
        odkPassword = 'openVA2018'
        odkFormID = 'va_who_v1_5_1'
        odkLastRun = '1901-01-01_00:00:01'
        odkLastRunDate = datetime.datetime.strptime(
            odkLastRun, '%Y-%m-%d_%H:%M:%S').strftime('%Y/%m/%d')
        odkLastRunDatePrev = (
            datetime.datetime.strptime(odkLastRunDate, '%Y/%m/%d') -
            datetime.timedelta(days=1)
        ).strftime('%Y/%m/%d')
        odkLastRunResult = 'fail'
        odkUseCentral = 'False'
        odkProjectNumber = '1'

        ntODK = collections.namedtuple('ntODK',
                                       ['odkID',
                                        'odkURL',
                                        'odkUser',
                                        'odkPassword',
                                        'odkFormID',
                                        'odkLastRun',
                                        'odkLastRunResult',
                                        'odkLastRunDate',
                                        'odkLastRunDatePrev',
                                        'odkUseCentral',
                                        'odkProjectNumber'])
        settingsODK = ntODK(odkID,
                            odkURL,
                            odkUser,
                            odkPassword,
                            odkFormID,
                            odkLastRun,
                            odkLastRunResult,
                            odkLastRunDate,
                            odkLastRunDatePrev,
                            odkUseCentral,
                            odkProjectNumber)

        shutil.copy('ODKFiles/previous_bc_export.csv', 'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv', 'ODKFiles/odkBCExportNew.csv')

        pipelineODK = odk.ODK(settingsODK, '.')
        pipelineODK.mergeToPrevExport()

    def test_unique_records_are_preserved(self):
        """Check mergeToPrevExport() includes all VA records from ODK BC export files."""

        hasAll = True
        with open('ODKFiles/odkBCExportPrev.csv') as fCombined:
            fCombinedLines = fCombined.readlines()
        with open('ODKFiles/previous_bc_export.csv') as fPrevious:
            fPreviousLines = fPrevious.readlines()
        with open('ODKFiles/another_bc_export.csv') as fAnother:
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

        self.assertFalse(os.path.isfile('ODKFiles/odkBCExportNew.csv'))

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors=True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        for bcLog in glob.glob('briefcase*.log'):
            os.remove('./' + bcLog)


class InvalidConnection(unittest.TestCase):
    """Check that proper execptions are raised."""

    @classmethod
    def setUpClass(cls):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors=True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')

        if not os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            downloadBriefcase()

        odkID = None
        odkURL = 'https://odk.swisstph.ch/ODKAggregateOpenVa'
        odkUser = 'odk_openva'
        odkPassword = 'openVA2018'
        odkFormID = 'va_who_v1_5_1'
        odkLastRun = '1901-01-01_00:00:01'
        odkLastRunDate = datetime.datetime.strptime(
            odkLastRun, '%Y-%m-%d_%H:%M:%S').strftime('%Y/%m/%d')
        odkLastRunDatePrev = (
            datetime.datetime.strptime(odkLastRunDate, '%Y/%m/%d') -
            datetime.timedelta(days=1)
        ).strftime('%Y/%m/%d')
        odkLastRunResult = 'fail'
        odkUseCentral = 'False'
        odkProjectNumber = '1'

        ntODK = collections.namedtuple('ntODK',
                                       ['odkID',
                                        'odkURL',
                                        'odkUser',
                                        'odkPassword',
                                        'odkFormID',
                                        'odkLastRun',
                                        'odkLastRunResult',
                                        'odkLastRunDate',
                                        'odkLastRunDatePrev',
                                        'odkUseCentral',
                                        'odkProjectNumber'])
        badSettingsODK = ntODK(odkID,
                               odkURL,
                               'WRONG',
                               odkPassword,
                               odkFormID,
                               odkLastRun,
                               odkLastRunResult,
                               odkLastRunDate,
                               odkLastRunDatePrev,
                               odkUseCentral,
                               odkProjectNumber)

        cls.pipelineODK = odk.ODK(badSettingsODK, '.')

    def test_ODK_bad_odkID(self):
        """Check for error if odkID parameter is invalid."""

        self.assertRaises(odk.ODKError, self.pipelineODK.briefcase)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors=True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        for bcLog in glob.glob('briefcase*.log'):
            os.remove('./' + bcLog)


if __name__ == '__main__':
    unittest.main(verbosity=2)
