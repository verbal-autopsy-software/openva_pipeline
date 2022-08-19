from openva_pipeline import odk

import datetime
import shutil
import os
import unittest
import collections

from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
# import context

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class CompleteFreshRun(unittest.TestCase):
    """Check successful completion from blank slate."""

    @classmethod
    def setUpClass(cls):
        if os.path.isfile('ODKFiles/odk_export_new.csv'):
            os.remove('ODKFiles/odk_export_new.csv')
        if os.path.isfile('ODKFiles/odk_export_prev.csv'):
            os.remove('ODKFiles/odk_export_prev.csv')

        odk_id = None
        odk_url = 'https://odk-central.swisstph.ch'
        odk_user = 'who.va.view@swisstph.ch'
        odk_password = 'WHOVAVi3w153!'
        odk_form_id = 'va_who_v1_5_3'
        odk_last_run = '1901-01-01_00:00:01'
        odk_last_run_date = datetime.datetime.strptime(
            odk_last_run, '%Y-%m-%d_%H:%M:%S').strftime('%Y/%m/%d')
        odk_last_run_date_prev = (
            datetime.datetime.strptime(odk_last_run_date, '%Y/%m/%d') -
            datetime.timedelta(days=1)
        ).strftime('%Y/%m/%d')
        odk_last_run_result = 'fail'
        odk_use_central = 'True'
        odk_project_number = '40'

        nt_odk = collections.namedtuple('nt_odk',
                                        ['odk_id',
                                         'odk_url',
                                         'odk_user',
                                         'odk_password',
                                         'odk_form_id',
                                         'odk_last_run',
                                         'odk_last_run_result',
                                         'odk_last_run_date',
                                         'odk_last_run_date_prev',
                                         'odk_use_central',
                                         'odk_project_number'])
        settings_odk = nt_odk(odk_id,
                              odk_url,
                              odk_user,
                              odk_password,
                              odk_form_id,
                              odk_last_run,
                              odk_last_run_result,
                              odk_last_run_date,
                              odk_last_run_date_prev,
                              odk_use_central,
                              odk_project_number)
        nt_pipeline = collections.namedtuple(
            "nt_pipeline",
            ["working_directory"]
        )
        settings_pipeline = nt_pipeline(".")
        settings = {"odk": settings_odk,
                    "pipeline": settings_pipeline}
        cls.pipeline_odk = odk.ODK(settings)
        cls.odk_central_return = cls.pipeline_odk.central()

    def test_odk_central_return(self):
        """Check successful run with valid parameters."""

        self.assertTrue("Downloaded" in self.odk_central_return)

    def test_for_file_odk_export_new(self):
        """Check for exported CSV file."""

        self.assertTrue(os.path.isfile('ODKFiles/odk_export_new.csv'))

    def test_merge_to_prev_export(self):
        """Check merge_to_prev_export() moves odk_export_new.csv to
        odk_export_prev.csv"""

        self.pipeline_odk.merge_to_prev_export()
        self.assertTrue(os.path.isfile('ODKFiles/odk_export_prev.csv'))

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile('ODKFiles/odk_export_new.csv'):
            os.remove('ODKFiles/odk_export_new.csv')
        if os.path.isfile('ODKFiles/odk_export_prev.csv'):
            os.remove('ODKFiles/odk_export_prev.csv')


class ProperMergeWithExistingExports(unittest.TestCase):
    """Check that unique VA records get preserved with new & exports."""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_export_new.csv'):
            os.remove('ODKFiles/odk_export_new.csv')
        if os.path.isfile('ODKFiles/odk_export_prev.csv'):
            os.remove('ODKFiles/odk_export_prev.csv')

        odk_id = None
        odk_url = 'https://odk-central.swisstph.ch'
        odk_user = 'who.va.view@swisstph.ch'
        odk_password = 'who.va.view@swisstph.ch'
        odk_form_id = 'va_who_v1_5_3'
        odk_last_run = '1901-01-01_00:00:01'
        odk_last_run_date = datetime.datetime.strptime(
            odk_last_run, '%Y-%m-%d_%H:%M:%S').strftime('%Y/%m/%d')
        odk_last_run_date_prev = (
            datetime.datetime.strptime(odk_last_run_date, '%Y/%m/%d') -
            datetime.timedelta(days=1)
        ).strftime('%Y/%m/%d')
        odk_last_run_result = 'fail'
        odk_use_central = 'True'
        odk_project_number = '40'

        nt_odk = collections.namedtuple('nt_odk',
                                        ['odk_id',
                                         'odk_url',
                                         'odk_user',
                                         'odk_password',
                                         'odk_form_id',
                                         'odk_last_run',
                                         'odk_last_run_result',
                                         'odk_last_run_date',
                                         'odk_last_run_date_prev',
                                         'odk_use_central',
                                         'odk_project_number'])
        settings_odk = nt_odk(odk_id,
                              odk_url,
                              odk_user,
                              odk_password,
                              odk_form_id,
                              odk_last_run,
                              odk_last_run_result,
                              odk_last_run_date,
                              odk_last_run_date_prev,
                              odk_use_central,
                              odk_project_number)
        nt_pipeline = collections.namedtuple(
            "nt_pipeline",
            ["working_directory"]
        )
        settings_pipeline = nt_pipeline(".")
        settings = {"odk": settings_odk,
                    "pipeline": settings_pipeline}

        shutil.copy('ODKFiles/previous_export.csv',
                    'ODKFiles/odk_export_prev.csv')
        shutil.copy('ODKFiles/another_export.csv',
                    'ODKFiles/odk_export_new.csv')

        pipeline_odk = odk.ODK(settings)
        pipeline_odk.merge_to_prev_export()

    def test_unique_records_are_preserved(self):
        """Check merge_to_prev_export() includes all VA records from
        ODK BC export files."""

        has_all = True
        with open('ODKFiles/odk_export_prev.csv') as f_combined:
            f_combined_lines = f_combined.readlines()
        with open('ODKFiles/previous_export.csv') as f_previous:
            f_previous_lines = f_previous.readlines()
        with open('ODKFiles/another_export.csv') as f_another:
            f_another_lines = f_another.readlines()
        for line in f_previous_lines:
            if line not in f_combined_lines:
                has_all = False
        for line in f_another_lines:
            if line not in f_combined_lines:
                has_all = False
        self.assertTrue(has_all)

    def test_merge_to_prev_export(self):
        """Check merge_to_prev_export() moves odk_export_new.csv to
        odk_export_prev.csv"""

        self.assertFalse(os.path.isfile('ODKFiles/odk_export_new.csv'))

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile('ODKFiles/odk_export_new.csv'):
            os.remove('ODKFiles/odk_export_new.csv')
        if os.path.isfile('ODKFiles/odk_export_prev.csv'):
            os.remove('ODKFiles/odk_export_prev.csv')


class InvalidConnection(unittest.TestCase):
    """Check that proper exceptions are raised."""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_export_new.csv'):
            os.remove('ODKFiles/odk_export_new.csv')
        if os.path.isfile('ODKFiles/odk_export_prev.csv'):
            os.remove('ODKFiles/odk_export_prev.csv')

        odk_id = None
        odk_url = 'https://odk-central.swisstph.ch'
        odk_password = 'who.va.view@swisstph.ch'
        odk_form_id = 'va_who_v1_5_3'
        odk_last_run = '1901-01-01_00:00:01'
        odk_last_run_date = datetime.datetime.strptime(
            odk_last_run, '%Y-%m-%d_%H:%M:%S').strftime('%Y/%m/%d')
        odk_last_run_date_prev = (
            datetime.datetime.strptime(odk_last_run_date, '%Y/%m/%d') -
            datetime.timedelta(days=1)
        ).strftime('%Y/%m/%d')
        odk_last_run_result = 'fail'
        odk_use_central = 'True'
        odk_project_number = '40'

        nt_odk = collections.namedtuple('nt_odk',
                                        ['odk_id',
                                         'odk_url',
                                         'odk_user',
                                         'odk_password',
                                         'odk_form_id',
                                         'odk_last_run',
                                         'odk_last_run_result',
                                         'odk_last_run_date',
                                         'odk_last_run_date_prev',
                                         'odk_use_central',
                                         'odk_project_number'])
        bad_settings_odk = nt_odk(odk_id,
                                  odk_url,
                                  'WRONG',
                                  odk_password,
                                  odk_form_id,
                                  odk_last_run,
                                  odk_last_run_result,
                                  odk_last_run_date,
                                  odk_last_run_date_prev,
                                  odk_use_central,
                                  odk_project_number)
        nt_pipeline = collections.namedtuple(
            "nt_pipeline",
            ["working_directory"]
        )
        settings_pipeline = nt_pipeline(".")
        bad_settings = {"odk": bad_settings_odk,
                        "pipeline": settings_pipeline}
        cls.pipeline_odk = odk.ODK(bad_settings)

    def test_odk_bad_odk_id(self):
        """Check for error if odk_id parameter is invalid."""

        self.assertRaises(odk.ODKError, self.pipeline_odk.central)

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile('ODKFiles/odk_export_new.csv'):
            os.remove('ODKFiles/odk_export_new.csv')
        if os.path.isfile('ODKFiles/odk_export_prev.csv'):
            os.remove('ODKFiles/odk_export_prev.csv')


if __name__ == '__main__':
    unittest.main(verbosity=2)
