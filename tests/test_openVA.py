from openva_pipeline.transfer_db import TransferDB
from openva_pipeline.pipeline import Pipeline
from openva_pipeline.openva import OpenVA
from openva_pipeline.runPipeline import download_smartva
from openva_pipeline.runPipeline import create_transfer_db
from openva_pipeline.exceptions import OpenVAError
from openva_pipeline.exceptions import SmartVAError
import unittest
import os
import shutil
import collections
from datetime import datetime
from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class CheckCopyVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odk_bc_export_new.csv')
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')

        pl = Pipeline(db_file_name='Pipeline.db',
                      db_directory='.',
                      db_key='enilepiP')
        settings = pl.config()
        cls.static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')

        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )
        r_openva = OpenVA(settings, cls.static_run_date)
        cls.zero_records = r_openva.copy_va()

    def test_copy_va_zero_records_false(self):
        """Check that copy_va() returns zero_records=False."""

        self.assertFalse(self.zero_records)

    def test_copy_va_isfile(self):
        """Check that copy_va() brings in new file."""

        self.assertTrue(
            os.path.isfile('OpenVAFiles/openva_input.csv')
        )

    def test_copy_va_merge(self):
        """Check that copy_va() includes all records."""

        has_all = True
        with open('OpenVAFiles/pycrossva_input.csv') as f_combined:
            f_combined_lines = f_combined.readlines()
        with open('ODKFiles/previous_bc_export.csv') as f_previous:
            f_previous_lines = f_previous.readlines()
        with open('ODKFiles/another_bc_export.csv') as f_another:
            f_another_lines = f_another.readlines()
        for line in f_previous_lines:
            if line not in f_combined_lines:
                has_all = False
        for line in f_another_lines:
            if line not in f_combined_lines:
                has_all = False
        self.assertTrue(has_all)

    @classmethod
    def tearDownClass(cls):

        os.remove('ODKFiles/odk_bc_export_prev.csv')
        os.remove('ODKFiles/odk_bc_export_new.csv')
        os.remove('OpenVAFiles/openva_input.csv')
        os.remove('Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )


class CheckZeroRecords(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/zero_records_bc_export.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/zero_records_bc_export.csv',
                    'ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('OpenVAFiles/openva_input.csv'):
            os.remove('OpenVAFiles/openva_input.csv')
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')

        pl = Pipeline(db_file_name='Pipeline.db',
                      db_directory='.',
                      db_key='enilepiP')
        settings = pl.config()
        cls.static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')

        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )

        r_openva = OpenVA(settings, cls.static_run_date)
        cls.zero_records = r_openva.copy_va()

    def test_copy_va_zero_records_true(self):
        """Check that copy_va() returns zero_records == True."""

        self.assertTrue(self.zero_records)

    def test_copy_va_zero_records_true_no_file(self):
        """Check that copy_va() does not produce file if zero records."""

        self.assertFalse(
            os.path.isfile('OpenVAFiles/openva_input.csv')
        )

    @classmethod
    def tearDownClass(cls):

        os.remove('ODKFiles/odk_bc_export_prev.csv')
        os.remove('ODKFiles/odk_bc_export_new.csv')
        os.remove('Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )


class CheckInSilicoVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('Check_InSilicoVA_Pipeline.db'):
            os.remove('Check_InSilicoVA_Pipeline.db')
        create_transfer_db('Check_InSilicoVA_Pipeline.db', '.', 'enilepiP')

        pl = Pipeline(db_file_name='Check_InSilicoVA_Pipeline.db',
                      db_directory='.',
                      db_key='enilepiP')
        fields = ['algorithm', 'algorithmMetadataCode']
        values = ['InSilicoVA',
                  ('InSilicoVA-2016|1.0.0|InterVA|5|'
                   '2016 WHO Verbal Autopsy Form|v1_4_1')]
        pl._update_pipeline(fields, values)
        pl.pipeline_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        settings = pl.config()
        cls.static_run_date = \
            datetime(2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
 
        cls.r_script = os.path.join('OpenVAFiles', cls.static_run_date,
                                    'Rscript_' + cls.static_run_date + '.R')
        cls.r_out_file = os.path.join('OpenVAFiles', cls.static_run_date,
                                      'Rscript_' + cls.static_run_date +
                                      '.Rout')
        r_openva = OpenVA(settings, cls.static_run_date)
        zero_records = r_openva.copy_va()
        r_openva.r_script()
        cls.completed = r_openva.get_cod()

    def test_insilicova_r_script(self):
        """Check that r_script() creates an R script for InSilicoVA."""

        self.assertTrue(os.path.isfile(self.rScript))

    def test_insilicova_r_out(self):
        """Check that get_cod() executes R script for InSilicoVA"""

        self.assertTrue(os.path.isfile(self.rOutFile))

    def test_insilicova_eav(self):
        """Check that get_cod() creates EAV outpue for InSilicoVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/entityAttributeValue.csv'))

    def test_insilicova_record_storage(self):
        """Check that get_cod() recordStorage.csv for InSilicoVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/recordStorage.csv'))

    def test_insilicova_return_code(self):
        """Check that get_cod() runs InSilicoVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )
        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        if os.path.isfile('OpenVAFiles/openva_input.csv'):
            os.remove('OpenVAFiles/openva_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")
        os.remove("Check_InSilicoVA_Pipeline.db")


class CheckInterVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('Check_InterVA_Pipeline.db'):
            os.remove('Check_InterVA_Pipeline.db')
        create_transfer_db('Check_InterVA_Pipeline.db', '.', 'enilepiP')

        # pipeline_run_date = datetime.now()
        pipelineRunDate = datetime(2018, 9, 1, 9, 0, 0). \
            strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(db_file_name='Check_InterVA_Pipeline.db',
                            db_directory='.',
                            db_key='enilepiP',
                            pl_run_date=pipelineRunDate)
        conn = xferDB.connect_db()
        c = conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InterVA','InterVA5|5|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        settingsPipeline = xferDB.config_pipeline(conn)
        settingsODK = xferDB.config_odk(conn)
        settingsInterVA = xferDB.config_openva(conn,
                                              'InterVA',
                                               settingsPipeline.workingDirectory)
        conn.close()
        cls.static_run_date = datetime(2018, 9, 1, 9, 0, 0). \
                            strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )
        cls.rScript = os.path.join('OpenVAFiles', cls.static_run_date,
                                   'Rscript_' + cls.static_run_date + '.R')
        cls.rOutFile = os.path.join('OpenVAFiles', cls.static_run_date,
                                    'Rscript_' + cls.static_run_date + '.Rout')
        r_openva = OpenVA(vaArgs=settingsInterVA,
                         pipelineArgs=settingsPipeline,
                         odkID=settingsODK.odkID,
                         runDate=cls.static_run_date)
        zero_records = r_openva.copy_va()
        r_openva.r_script()
        cls.completed = r_openva.get_cod()

    def test_interva_rscript(self):
        """Check that get_cod() executes R script for InterVA"""

        self.assertTrue(os.path.isfile(self.rScript))

    def test_interva_rOut(self):
        """Check that get_cod() executes R script for InterVA"""

        self.assertTrue(os.path.isfile(self.rOutFile))

    def test_interva_EAV(self):
        """Check that get_cod() creates EAV outpue for InterVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/entityAttributeValue.csv'))

    def test_interva_recordStorage(self):
        """Check that get_cod() recordStorage.csv for InterVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/recordStorage.csv'))

    def test_interva_returncode(self):
        """Check that get_cod() runs InterVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        os.remove('Check_InterVA_Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )
        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        if os.path.isfile('OpenVAFiles/openva_input.csv'):
            os.remove('OpenVAFiles/openva_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_SmartVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-1.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-2.csv',
                    'ODKFiles/odk_bc_export_new.csv')
        if not os.path.isfile('smartva'):
            downloadSmartVA()
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')

        # pipeline_run_date = datetime.now()
        pipelineRunDate = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(db_file_name='copy_Pipeline.db',
                            db_directory='.',
                            db_key='enilepiP',
                            pl_run_date=pipelineRunDate)
        conn = xferDB.connect_db()
        c = conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('SmartVA', 'SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1')
        c.execute(sql, par)
        settingsPipeline = xferDB.config_pipeline(conn)
        settingsODK = xferDB.config_odk(conn)
        settingsSmartVA = xferDB.config_openva(conn,
                                              'SmartVA',
                                               settingsPipeline.workingDirectory)
        conn.rollback()
        conn.close()
        cls.static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )
        cliSmartVA = OpenVA(vaArgs=settingsSmartVA,
                            pipelineArgs=settingsPipeline,
                            odkID=settingsODK.odkID,
                            runDate=cls.static_run_date)
        zero_records = cliSmartVA.copy_va()
        cls.completed = cliSmartVA.get_cod()
        cls.svaOut = os.path.join(
            'OpenVAFiles',
            cls.static_run_date,
            '1-individual-cause-of-death/individual-cause-of-death.csv'
        )

    def test_smartva(self):
        """Check that get_cod() executes smartva cli"""

        self.assertTrue(os.path.isfile(self.svaOut))

    def test_smartva_EAV(self):
        """Check that get_cod() creates EAV outpue for SmartVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/entityAttributeValue.csv'))

    def test_smartva_recordStorage(self):
        """Check that get_cod() recordStorage.csv for SmartVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/recordStorage.csv'))

    def test_smartva_returncode(self):
        """Check that get_cod() runs SmartVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.static_run_date),
            ignore_errors=True
        )
        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        if os.path.isfile('OpenVAFiles/openva_input.csv'):
            os.remove('OpenVAFiles/openva_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_Exceptions_InSilicoVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-1.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-2.csv',
                    'ODKFiles/odk_bc_export_new.csv')

       
    def setUp(self):
        
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(db_file_name='copy_Pipeline.db',
                            db_directory='.',
                            db_key='enilepiP',
                            pl_run_date=static_run_date)
        conn = xferDB.connect_db()
        c = conn.cursor()
        algorithm = 'InSilicoVA'
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InSilicoVA', 'InSilicoVA|1.1.4|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        sql = 'UPDATE InSilicoVA_Conf SET data_type = ?'
        par = ('WHO2012',)
        c.execute(sql, par)
        settingsPipeline = xferDB.config_pipeline(conn)
        settingsAlgorithm = xferDB.config_openva(conn,
                                                 algorithm,
                                                 settingsPipeline.workingDirectory)

        self.r_openva = OpenVA(vaArgs=settingsAlgorithm,
                              pipelineArgs=settingsPipeline,
                              odkID=None,
                              runDate=static_run_date)
        zero_records = self.r_openva.copy_va()
        self.r_openva.r_script()
        conn.rollback()
        conn.close()

    def test_insilico_exception(self):
        """get_cod() raises exception with faulty R script for InSilicoVA."""
        
        self.assertRaises(OpenVAError, self.r_openva.get_cod)

    def tearDown(self):
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', static_run_date),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):
    
        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        if os.path.isfile('OpenVAFiles/openva_input.csv'):
            os.remove('OpenVAFiles/openva_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_Exceptions_InterVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-1.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-2.csv',
                    'ODKFiles/odk_bc_export_new.csv')

       
    def setUp(self):
        
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(db_file_name='copy_Pipeline.db',
                            db_directory='.',
                            db_key='enilepiP',
                            pl_run_date=static_run_date)
        conn = xferDB.connect_db()
        c = conn.cursor()
        algorithm = 'InterVA'
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InterVA', 'InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        sql = 'UPDATE InterVA_Conf SET version = ?'
        par = ('4',)
        c.execute(sql, par)
        settingsPipeline = xferDB.config_pipeline(conn)
        settingsAlgorithm = xferDB.config_openva(conn,
                                                 algorithm,
                                                 settingsPipeline.workingDirectory)

        self.r_openva = OpenVA(vaArgs=settingsAlgorithm,
                              pipelineArgs=settingsPipeline,
                              odkID=None,
                              runDate=static_run_date)
        zero_records = self.r_openva.copy_va()
        self.r_openva.r_script()
        conn.rollback()
        conn.close()

    def test_interva_exception(self):
        """get_cod() should raise an exception with problematic Interva R script."""

        self.assertRaises(OpenVAError, self.r_openva.get_cod)

    def tearDown(self):
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', static_run_date),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):
    
        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        if os.path.isfile('OpenVAFiles/openva_input.csv'):
            os.remove('OpenVAFiles/openva_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_Exceptions_SmartVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-1.csv',
                    'ODKFiles/odk_bc_export_prev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-2.csv',
                    'ODKFiles/odk_bc_export_new.csv')
        if not os.path.isfile('smartva'):
            downloadSmartVA()
       
    def setUp(self):
        
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(db_file_name='copy_Pipeline.db',
                            db_directory='.',
                            db_key='enilepiP',
                            pl_run_date=static_run_date)
        conn = xferDB.connect_db()
        c = conn.cursor()
        algorithm = 'SmartVA'
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('SmartVA', 'SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1')
        c.execute(sql, par)
        ntSmartVA = collections.namedtuple("ntSmartVA",
                                           ["SmartVA_country",
                                            "SmartVA_hiv",
                                            "SmartVA_malaria",
                                            "SmartVA_hce",
                                            "SmartVA_freetext",
                                            "SmartVA_figures",
                                            "SmartVA_language"])
        settingsAlgorithm = ntSmartVA("Unknown",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong")
        settingsPipeline = xferDB.config_pipeline(conn)

        self.r_openva = OpenVA(vaArgs=settingsAlgorithm,
                              pipelineArgs=settingsPipeline,
                              odkID=None,
                              runDate=static_run_date)
        zero_records = self.r_openva.copy_va()
        conn.rollback()
        conn.close()

    def test_smartva_exception(self):
        """get_cod() should raise an exception with faulty args for smartva cli"""

        self.assertRaises(SmartVAError, self.r_openva.get_cod)

    def tearDown(self):
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', static_run_date),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):
    
        if os.path.isfile('ODKFiles/odk_bc_export_new.csv'):
            os.remove('ODKFiles/odk_bc_export_new.csv')
        if os.path.isfile('ODKFiles/odk_bc_export_prev.csv'):
            os.remove('ODKFiles/odk_bc_export_prev.csv')
        if os.path.isfile('OpenVAFiles/openva_input.csv'):
            os.remove('OpenVAFiles/openva_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


if __name__ == '__main__':
    unittest.main(verbosity=2)
