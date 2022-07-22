from openva_pipeline.transfer_db import TransferDB
from openva_pipeline.run_pipeline import create_transfer_db
from openva_pipeline.exceptions import DatabaseConnectionError
from openva_pipeline.exceptions import PipelineConfigurationError
from openva_pipeline.exceptions import ODKConfigurationError
from openva_pipeline.exceptions import OpenVAConfigurationError
from openva_pipeline.exceptions import DHISConfigurationError
import unittest
import os
import shutil
import datetime
from pandas import read_csv
from sys import path

source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
os.chdir(os.path.abspath(os.path.dirname(__file__)))


class CheckDBHasTables(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        pipeline_run_date = datetime.datetime.now()
        xfer_db = TransferDB(db_file_name='Pipeline.db', db_directory='.',
                             db_key='enilepiP', pl_run_date=pipeline_run_date)
        conn = xfer_db.connect_db()
        c = conn.cursor()
        sql_test_connection = ("SELECT name FROM SQLITE_MASTER "
                               "where type = 'table';")
        c.execute(sql_test_connection)
        table_names = c.fetchall()
        cls.table_names = table_names
        cls.list_table_names = [j for i in table_names for j in i]

    def test_connection_has_tables(self):
        """Test that the pipeline can connect to the DB."""
        self.assertTrue(len(self.table_names) > 0)

    def test_table_pipeline_conf(self):
        """Test that the Pipeline.db has Pipeline_Conf table."""
        self.assertIn('Pipeline_Conf', self.list_table_names)

    def test_table_va_storage(self):
        """Test that the Pipeline.db has VA_Storage table."""
        self.assertIn('VA_Storage', self.list_table_names)

    def test_table_eventLog(self):
        """Test that the Pipeline.db has EventLog table."""
        self.assertIn('EventLog', self.list_table_names)

    def test_table_odk_conf(self):
        """Test that the Pipeline.db has ODK_Conf table."""
        self.assertIn('ODK_Conf', self.list_table_names)

    def test_table_interva_conf(self):
        """Test that the Pipeline.db has InterVA_Conf table."""
        self.assertIn('InterVA_Conf', self.list_table_names)

    def test_table_advanced_interva_conf(self):
        """Test that the Pipeline.db has Advanced_InterVA_Conf table."""
        self.assertIn('Advanced_InterVA_Conf', self.list_table_names)

    def test_table_insilicova_conf(self):
        """Test that the Pipeline.db has InSilicoVA_Conf table."""
        self.assertIn('InSilicoVA_Conf', self.list_table_names)

    def test_table_advanced_insilicova_conf(self):
        """Test that the Pipeline.db has Advanced_InSilicoVA_Conf table."""
        self.assertIn('Advanced_InSilicoVA_Conf', self.list_table_names)

    def test_table_smartva_conf(self):
        """Test that the Pipeline.db has SmartVA_Conf table."""
        self.assertIn('SmartVA_Conf', self.list_table_names)

    def test_table_smartva_country(self):
        """Test that the Pipeline.db has SmartVA_Country table."""
        self.assertIn('SmartVA_Country', self.list_table_names)

    def test_table_dhis_conf(self):
        """Test that the Pipeline.db has DHIS_Conf table."""
        self.assertIn('DHIS_Conf', self.list_table_names)

    def test_table_cod_codes_dhis(self):
        """Test that the Pipeline.db has COD_Codes_DHIS table."""
        self.assertIn('COD_Codes_DHIS', self.list_table_names)

    def test_table_algorithm_metadata_options(self):
        """Test that the Pipeline.db has Algorithm_Metadata_Options table."""
        self.assertIn('Algorithm_Metadata_Options', self.list_table_names)

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckDBConnectionExceptions(unittest.TestCase):

    def test_db_file_present_exception(self):
        """Check that DB file is located in path."""
        bad_path = '/invalid/path/to/pipelineDB'
        pipeline_run_date = datetime.datetime.now()
        xfer_db = TransferDB(db_file_name='Pipeline.db', db_directory=bad_path,
                             db_key='enilepiP', pl_run_date=pipeline_run_date)
        self.assertRaises(DatabaseConnectionError, xfer_db.connect_db)

    def test_wrong_key_exception(self):
        """Pipeline should raise an error when the wrong key is used."""
        pipeline_run_date = datetime.datetime.now()
        xfer_db = TransferDB(db_file_name='Pipeline.db', db_directory='.',
                             db_key='wrong_db_key', 
                             pl_run_date=pipeline_run_date)
        self.assertRaises(DatabaseConnectionError, xfer_db.connect_db)


class CheckPipelineConf(unittest.TestCase):
    """Test methods that grab configuration settings for pipeline."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_file_name = 'Pipeline.db'
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()

        cls.xfer_db = TransferDB(db_file_name=db_file_name,
                                 db_directory=db_directory,
                                 db_key=db_key,
                                 pl_run_date=pipeline_run_date)
        cls.conn = cls.xfer_db.connect_db()
        c = cls.conn.cursor()
        c.execute('SELECT dhisCode from Algorithm_Metadata_Options;')
        cls.meta_data_query = c.fetchall()
        cls.settings_pipeline = cls.xfer_db.config_pipeline(cls.conn)

        cls.copy_xfer_db = TransferDB(db_file_name='copy_Pipeline.db',
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db.connect_db()

        # parameters for connecting to DB with wrong Tables
        wrong_tables_db_file_name = 'wrongTables_Pipeline.db'
        cls.wrong_tables_xfer_db = TransferDB(
            db_file_name=wrong_tables_db_file_name,
            db_directory=db_directory,
            db_key=db_key,
            pl_run_date=pipeline_run_date)
        cls.wrong_tables_conn = cls.wrong_tables_xfer_db.connect_db()
        cls.wrong_tables_c = cls.wrong_tables_conn.cursor()

        # parameters for connecting to DB with wrong fields
        wrong_fields_db_file_name = 'wrongFields_Pipeline.db'
        cls.wrong_fields_xfer_db = TransferDB(
            db_file_name=wrong_fields_db_file_name,
            db_directory=db_directory,
            db_key=db_key,
            pl_run_date=pipeline_run_date)
        cls.wrong_fields_conn = cls.wrong_fields_xfer_db.connect_db()

    def test_pipeline_conf_exception_notable(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(PipelineConfigurationError,
                          self.wrong_tables_xfer_db.config_pipeline,
                          self.wrong_tables_conn
                          )

    def test_pipeline_conf_exception_no_field(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(PipelineConfigurationError,
                          self.wrong_fields_xfer_db.config_pipeline,
                          self.wrong_fields_conn
                          )

    # Thinking about removing this check -- 2021-06-09 (jt)
    # def test_pipeline_conf_algorithmMetadataCode(self):
    #     """Test Pipeline_Conf table has valid algorithmMetadataCode."""

    #     validMetadataCode = self.settings_pipeline.algorithmMetadataCode in \
    #         [j for i in self.metadataQuery for j in i]
    #     self.assertTrue(validMetadataCode)

    # def test_pipeline_conf_algorithmMetadataCode_Exception_value(self):
    #     """config_pipeline should fail with invalid algorithmMetadataCode."""

    #     c = self.copy_conn.cursor()
    #     sql = 'UPDATE Pipeline_Conf SET algorithmMetadataCode = ?'
    #     par = ('wrong',)
    #     c.execute(sql, par)
    #     self.assertRaises(PipelineConfigurationError,
    #                       self.copy_xfer_db.config_pipeline, self.copy_conn)
    #     self.copy_conn.rollback()

    def test_pipeline_conf_cod_source(self):
        """Test Pipeline_Conf table has valid cod_source"""
        valid_cod_source = self.settings_pipeline.cod_source in \
            ('ICD10', 'WHO', 'Tariff')
        self.assertTrue(valid_cod_source)

    def test_pipeline_conf_cod_source_exception(self):
        """config_pipeline should fail with invalid codSource."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET codSource = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xfer_db.config_pipeline, self.copy_conn)
        self.copy_conn.rollback()

    def test_pipeline_conf_algorithm(self):
        """Test Pipeline_Conf table has valid algorithm"""
        valid_algorithm = self.settings_pipeline.algorithm in \
            ('InSilicoVA', 'InterVA', 'SmartVA')
        self.assertTrue(valid_algorithm)

    def test_pipeline_conf_algorithm_exception(self):
        """config_pipeline should fail with invalid algorithm."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xfer_db.config_pipeline, self.copy_conn)
        self.copy_conn.rollback()

    def test_pipeline_conf_working_directory(self):
        """Test Pipeline_Conf table has valid algorithm"""
        valid_wd = os.path.isdir(self.settings_pipeline.working_directory)
        self.assertTrue(valid_wd)

    def test_pipeline_conf_working_directory_exception(self):
        """config_pipeline should fail with invalid working_directory."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET workingDirectory = ?'
        par = ('/wrong/path',)
        c.execute(sql, par)
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xfer_db.config_pipeline, self.copy_conn)
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckODKConf(unittest.TestCase):
    """Test methods that grab ODK configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_file_name = 'Pipeline.db'
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        conn = xfer_db.connect_db()
        cls.settings_odk = xfer_db.config_odk(conn)

        cls.copy_xfer_db = TransferDB(db_file_name='copy_Pipeline.db',
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db.connect_db()

    def test_odk_conf_odk_url(self):
        """Test ODK_Conf table has valid odkURL"""
        self.assertEqual(self.settings_odk.odk_url,
                         'https://odk-central.swisstph.ch')

    def test_odk_conf_odk_url_exception(self):
        """config_odk should fail with invalid url."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE ODK_Conf SET odkURL = ?'
        par = ('wrong.url',)
        c.execute(sql, par)
        self.assertRaises(ODKConfigurationError,
                          self.copy_xfer_db.config_odk, self.copy_conn)
        self.copy_conn.rollback()

    def test_odk_conf_odk_user(self):
        """Test ODK_Conf table has valid odkUser"""
        self.assertEqual(self.settings_odk.odk_user, 'who.va.view@swisstph.ch')

    def test_odk_conf_odk_password(self):
        """Test ODK_Conf table has valid odkPassword"""
        self.assertEqual(self.settings_odk.odk_password, 'WHOVAVi3w153!')

    def test_odk_conf_odk_form_id(self):
        """Test ODK_Conf table has valid odkFormID"""
        self.assertEqual(self.settings_odk.odk_form_id, 'va_who_v1_5_3')

    def test_odk_conf_odk_last_run(self):
        """Test ODK_Conf table has valid odkLastRun"""
        self.assertEqual(self.settings_odk.odk_last_run, '1900-01-01_00:00:01')

    def test_odk_conf_odk_last_run_date(self):
        """Test ODK_Conf table has valid odkLastRunDate"""
        self.assertEqual(self.settings_odk.odk_last_run_date, '1900/01/01')

    def test_odk_conf_odk_last_run_date_prev(self):
        """Test ODK_Conf table has valid odk_last_run_date_prev"""
        self.assertEqual(self.settings_odk.odk_last_run_date_prev,
                         '1899/12/31')

    def test_odk_conf_odk_use_central(self):
        """Test ODK_Conf table has valid odkUseCentral"""
        self.assertEqual(self.settings_odk.odk_use_central, 'True')

    def test_odk_conf_odk_project_number(self):
        """Test ODK_Conf table has valid odkProjectNumber"""
        self.assertEqual(self.settings_odk.odk_project_number, '40')

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckOpenVAConfInterVA(unittest.TestCase):
    """Test methods that grab InterVA configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_file_name = 'Pipeline.db'
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        conn = xfer_db.connect_db()
        cls.settings_pipeline = xfer_db.config_pipeline(conn)
        cls.settings_openva = xfer_db.config_openva(conn, 'InterVA')

        cls.copy_xfer_db = TransferDB(db_file_name='copy_Pipeline.db',
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db.connect_db()

    def test_openva_conf_interva_version(self):
        """Test InterVA_Conf table has valid version"""
        self.assertIn(self.settings_openva.interva_version, ('4', '5'))

    def test_openva_conf_interva_version_exception(self):
        """config_openva should fail with invalid 
        InterVA_Conf.Version value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InterVA_Conf SET version = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_hiv(self):
        """Test InterVA_Conf table has valid HIV"""
        self.assertIn(self.settings_openva.interva_hiv, ('v', 'l', 'h'))

    def test_openva_conf_interva_hiv_exception(self):
        """config_openva should fail with invalid InterVA_Conf.HIV value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InterVA_Conf SET HIV = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_malaria(self):
        """Test InterVA_Conf table has valid Malaria"""
        self.assertIn(self.settings_openva.interva_malaria, ('v', 'l', 'h'))

    def test_openva_conf_interva_malaria_exception(self):
        """config_openva should fail with invalid 
        InterVA_Conf.Malaria value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InterVA_Conf SET Malaria = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_output(self):
        """Test Advanced_InterVA_Conf table has valid output."""
        self.assertIn(self.settings_openva.interva_output,
                      ('classic', 'extended')
                      )

    def test_openva_conf_interva_output_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.output value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET output = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_append(self):
        """Test Advanced_InterVA_Conf table has valid append."""
        self.assertIn(self.settings_openva.interva_append,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_interva_append_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.append value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET append = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_groupcode(self):
        """Test Advanced_InterVA_Conf table has valid groupcode."""
        self.assertIn(self.settings_openva.interva_groupcode,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_interva_groupcode_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.groupcode value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET groupcode = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_replicate(self):
        """Test Advanced_InterVA_Conf table has valid replicate."""
        self.assertIn(self.settings_openva.interva_replicate,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_interva_replicate_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.replicate value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET replicate = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_replicate_bug1(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug1."""
        self.assertIn(self.settings_openva.interva_replicate_bug1,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_interva_replicate_bug1_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.replicate_bug1 value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET replicate_bug1 = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    def test_openva_conf_interva_replicate_bug2(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug2."""
        self.assertIn(self.settings_openva.interva_replicate_bug2,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_interva_replicate_bug2_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.replicate_bug2 value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET replicate_bug2 = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InterVA')
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckOpenVAConfInSilicoVA(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_file_name = 'Pipeline.db'
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        conn = xfer_db.connect_db()
        cls.settings_pipeline = xfer_db.config_pipeline(conn)
        cls.settings_openva = xfer_db.config_openva(conn, 'InSilicoVA')

        cls.copy_xfer_db = TransferDB(db_file_name='copy_Pipeline.db',
                                     db_directory=db_directory,
                                     db_key=db_key,
                                     pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db.connect_db()

    def test_openva_conf_insilicova_data_type(self):
        """Test InSilicoVA_Conf table has valid data_type"""
        self.assertIn(self.settings_openva.insilicova_data_type,
                      ('WHO2012', 'WHO2016')
                      )

    def test_openva_conf_insilicova_data_type_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.data_type value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InSilicoVA_Conf SET data_type = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_nsim(self):
        """Test InSilicoVA_Conf table has valid Nsim"""
        self.assertEqual(self.settings_openva.insilicova_nsim, '4000')

    def test_openva_conf_insilicova_nsim_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.Nsim value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InSilicoVA_Conf SET Nsim = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_is_numeric(self):
        """Test InSilicoVA_Conf table has valid isNumeric"""
        self.assertIn(self.settings_openva.insilicova_is_numeric,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_is_numeric_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.isNumeric value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_insilicova_Conf SET isNumeric = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_update_cond_prob(self):
        """Test InSilicoVA_Conf table has valid updateCondProb"""
        self.assertIn(self.settings_openva.insilicova_update_cond_prob,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_update_cond_prob_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.updateCondProb value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_insilicova_Conf SET updateCondProb = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_keep_probbase_level(self):
        """Test InSilicoVA_Conf table has valid keepProbbase_level"""
        self.assertIn(self.settings_openva.insilicova_keep_probbase_level,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_keep_probbase_level_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.keepProbbase_level value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET keepProbbase_level = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_cond_prob(self):
        """Test InSilicoVA_Conf table has valid CondProb"""
        self.assertEqual(self.settings_openva.insilicova_cond_prob, 'NULL')

    def test_openva_conf_insilicova_cond_prob_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.CondProb value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET CondProb = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_cond_prob_num(self):
        """Test InSilicoVA_Conf table has valid CondProbNum"""
        self.assertEqual(self.settings_openva.insilicova_cond_prob_num, 'NULL')

    def test_openva_conf_insilicova_cond_prob_num_2(self):
        """config_openva should accept InSilicoVA_Conf.CondProbNum '.35'."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET CondProbNum = ?'
        par = ('.35',)
        c.execute(sql, par)
        new_test = self.copy_xfer_db.config_openva(
            self.copy_conn,
            'InSilicoVA')
        self.assertEqual(new_test.insilicova_cond_prob_num, '.35')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_cond_prob_num_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.CondProbNum value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET CondProbNum = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_datacheck(self):
        """Test InSilicoVA_Conf table has valid datacheck"""
        self.assertIn(self.settings_openva.insilicova_datacheck,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_datacheck_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.datacheck value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET datacheck = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_datacheck_missing(self):
        """Test InSilicoVA_Conf table has valid datacheck_missing"""
        self.assertIn(self.settings_openva.insilicova_datacheck_missing,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_datacheck_missing_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.datacheck_missing value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET datacheck_missing = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_external_sep(self):
        """Test InSilicoVA_Conf table has valid external_sep"""
        self.assertIn(self.settings_openva.insilicova_external_sep,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_external_sep_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.external_sep value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET external_sep = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_thin(self):
        self.assertEqual(self.settings_openva.insilicova_thin, '10')

    def test_openva_conf_insilicova_thin_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.thin value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET thin = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_burnin(self):
        """Test InSilicoVA_Conf table has valid burnin"""
        self.assertEqual(self.settings_openva.insilicova_burnin, '2000')

    def test_openva_conf_insilicova_burnin_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.burnin value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET burnin = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_auto_length(self):
        """Test InSilicoVA_Conf table has valid auto_length"""
        self.assertIn(self.settings_openva.insilicova_auto_length,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_auto_length_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.auto_length value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET auto_length = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_conv_csmf(self):
        """Test InSilicoVA_Conf table has valid conv_csmf"""
        self.assertEqual(self.settings_openva.insilicova_conv_csmf, '0.02')

    def test_openva_conf_insilicova_conv_csmf_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.conv_csmf value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET conv_csmf = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_jump_scale(self):
        """Test InSilicoVA_Conf table has valid jump_scale"""
        self.assertEqual(self.settings_openva.insilicova_jump_scale, '0.1')

    def test_openva_conf_insilicova_jump_scale_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.jump_scale value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET jump_scale = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_levels_prior(self):
        self.assertEqual(self.settings_openva.insilicova_levels_prior, 'NULL')
        
    def test_openva_conf_insilicova_levels_prior_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.levels_prior value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET levels_prior = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_levels_strength(self):
        self.assertEqual(self.settings_openva.insilicova_levels_strength, '1')

    def test_openva_conf_insilicova_levels_strength_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.levels_strength value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET levels_strength = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_trunc_min(self):
        self.assertEqual(self.settings_openva.insilicova_trunc_min, '0.0001')

    def test_openva_conf_insilicova_trunc_min_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.trunc_min value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET trunc_min = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_trunc_max(self):
        self.assertEqual(self.settings_openva.insilicova_trunc_max, '0.9999')

    def test_openva_conf_insilicova_trunc_max_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.trunc_max value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET trunc_max = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_subpop(self):
        self.assertEqual(self.settings_openva.insilicova_subpop, 'NULL')

    def test_openva_conf_insilicova_subpop_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.subpop value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET subpop = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_java_option(self):
        self.assertEqual(self.settings_openva.insilicova_java_option, '-Xmx1g')

    def test_openva_conf_insilicova_java_option_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.java_option value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET java_option = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_seed(self):
        self.assertEqual(self.settings_openva.insilicova_seed, '1')

    def test_openva_conf_insilicova_seed_Exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.seed value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET seed = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_phy_code(self):
        self.assertEqual(self.settings_openva.insilicova_phy_code, 'NULL')

    def test_openva_conf_insilicova_phy_code_Exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_code value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_code = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_phy_cat(self):
        self.assertEqual(self.settings_openva.insilicova_phy_cat, 'NULL')

    def test_openva_conf_insilicova_phy_cat_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_cat value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_cat = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_phy_unknown(self):
        self.assertEqual(self.settings_openva.insilicova_phy_unknown, 'NULL')

    def test_openva_conf_insilicova_phy_unknown_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_unknown value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_unknown = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_phy_external(self):
        self.assertEqual(self.settings_openva.insilicova_phy_external, 'NULL')

    def test_openva_conf_insilicova_phy_external_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_external value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_external = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_phy_debias(self):
        self.assertEqual(self.settings_openva.insilicova_phy_debias, 'NULL')

    def test_openva_conf_insilicova_phy_debias_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_debias value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_debias = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_exclude_impossible_cause(self):
        self.assertIn(self.settings_openva.insilicova_exclude_impossible_cause,
                      ('subset', 'all', 'InterVA', 'none')
                      )

    def test_openva_conf_insilicova_exclude_impossible_cause_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.exclude_impossible_cause value."""
        c = self.copy_conn.cursor()
        sql = ('UPDATE Advanced_InSilicoVA_Conf SET '
               'exclude_impossible_cause = ?')
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_no_is_missing(self):
        self.assertIn(self.settings_openva.insilicova_no_is_missing,
                      ('TRUE', 'FALSE')
                      )

    def test_openva_conf_insilicova_no_is_missing_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.no_is_missing value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET no_is_missing = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_indiv_ci(self):
        self.assertEqual(self.settings_openva.insilicova_indiv_ci, 'NULL')

    def test_openva_conf_insilicova_indiv_ci_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.indiv_CI value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET indiv_CI = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    def test_openva_conf_insilicova_groupcode(self):
        self.assertIn(self.settings_openva.insilicova_groupcode,
                      ('TRUE', 'FALSE'))

    def test_openva_conf_insilicova_groupcode_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.groupcode value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET groupcode = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'InSilicoVA')
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckSmartVAConf(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_file_name = 'Pipeline.db'
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        conn = xfer_db.connect_db()
        cls.settings_pipeline = xfer_db.config_pipeline(conn)
        cls.settings_smartva = xfer_db.config_openva(conn, 'SmartVA')

        cls.copy_xfer_db = TransferDB(db_file_name='copy_Pipeline.db',
                                     db_directory=db_directory,
                                     db_key=db_key,
                                     pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db.connect_db()

    def test_smartva_conf_country(self):
        """Test SmartVA_Conf table has valid country"""
        self.assertEqual(self.settings_smartva.smartva_country, 'Unknown')

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.country value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET country = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'SmartVA',
                          )
        self.copy_conn.rollback()

    def test_smartva_conf_hiv(self):
        """Test SmartVA_Conf table has valid hiv"""
        self.assertEqual(self.settings_smartva.smartva_hiv, 'False')

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.hiv value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET hiv = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'SmartVA',
                          )
        self.copy_conn.rollback()

    def test_smartva_conf_malaria(self):
        """Test SmartVA_Conf table has valid malaria"""
        self.assertEqual(self.settings_smartva.smartva_malaria, 'False')

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.malaria value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET malaria = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'SmartVA',
                          )
        self.copy_conn.rollback()

    def test_smartva_conf_hce(self):
        """Test SmartVA_Conf table has valid hce"""
        self.assertEqual(self.settings_smartva.smartva_hce, 'False')

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid SmartVA_Conf.hce value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET hce = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'SmartVA',
                          )
        self.copy_conn.rollback()

    def test_smartva_conf_freetext(self):
        """Test SmartVA_Conf table has valid freetext"""
        self.assertEqual(self.settings_smartva.smartva_freetext, 'False')

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.freetext value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET freetext = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'SmartVA',
                          )
        self.copy_conn.rollback()

    def test_smartva_conf_figures(self):
        """Test SmartVA_Conf table has valid figures"""
        self.assertEqual(self.settings_smartva.smartva_figures, 'False')

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.figures value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET figures = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'SmartVA',
                          )
        self.copy_conn.rollback()

    def test_smartva_conf_language(self):
        """Test SmartVA_Conf table has valid language"""
        self.assertEqual(self.settings_smartva.smartva_language, 'english')

    def test_smartva_conf_language_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.language value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET language = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          self.copy_conn, 'SmartVA',
                          )
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckDHISConf(unittest.TestCase):
    """Test methods that grab DHIS configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_file_name = 'Pipeline.db'
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()
        cls.algorithm = 'InterVA'

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        conn = xfer_db.connect_db()
        cls.settings_dhis = xfer_db.config_dhis(conn, cls.algorithm)

        cls.copy_xfer_db = TransferDB(db_file_name='copy_Pipeline.db',
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db.connect_db()

    def test_dhis_conf_dhis_url(self):
        """Test DHIS_Conf table has valid dhisURL"""
        self.assertEqual(self.settings_dhis[0].dhis_url,
                         'https://va30tr.swisstph-mis.ch')

    def test_dhis_conf_dhis_url_exception(self):
        """config_dhis should fail with invalid url."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisURL = ?'
        par = ('wrong.url',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_dhis_conf_dhis_user(self):
        """Test DHIS_Conf table has valid dhisUser"""
        self.assertEqual(self.settings_dhis[0].dhis_user, 'va-demo')

    def test_dhis_conf_dhis_user_exception(self):
        """config_dhis should fail with invalid dhisUser."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisUser = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_dhis_conf_dhis_password(self):
        """Test DHIS_Conf table has valid dhisPassword"""
        self.assertEqual(self.settings_dhis[0].dhis_password,
                         'VerbalAutopsy99!')

    def test_dhis_conf_dhis_password_exception(self):
        """config_dhis should fail with invalid dhisPassword."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisPassword = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_dhis_conf_dhis_org_unit(self):
        """Test DHIS_Conf table has valid dhisOrgUnit"""
        self.assertEqual(self.settings_dhis[0].dhis_org_unit, 'SCVeBskgiK6')

    def test_dhis_conf_dhis_post_root(self):
        """Test DHIS_Conf table has valid dhisPostRoot"""
        self.assertEqual(self.settings_dhis[0].dhis_post_root, 'False')

    def test_dhis_conf_dhis_org_unit_exception(self):
        """config_dhis should fail with invalid dhisOrgUnit."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisOrgUnit = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckDHISStoreVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        shutil.copy('OpenVAFiles/sample_new_storage.csv',
                    'OpenVAFiles/new_storage.csv')

        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_file_name = 'Pipeline.db'
        db_key = 'enilepiP'
        pipeline_run_date = datetime.datetime.now()
        wrong_db_key = 'wrongKey'
        db_directory = '.'

        cls.xfer_db = TransferDB(db_file_name=db_file_name,
                                 db_directory=db_directory,
                                 db_key=db_key,
                                 pl_run_date=pipeline_run_date)
        cls.conn = cls.xfer_db.connect_db()
        cls.xfer_db.config_pipeline(cls.conn)

    def test_dhis_store_va(self):
        """Check that VA records get stored in Transfer DB."""

        self.xfer_db.store_va(self.conn)
        c = self.conn.cursor()
        sql = 'SELECT id FROM VA_Storage'
        c.execute(sql)
        va_ids = c.fetchall()
        va_ids_list = [j for i in va_ids for j in i]
        s1 = set(va_ids_list)
        df_new_storage = read_csv('OpenVAFiles/new_storage.csv')
        df_new_storage_id = df_new_storage['odkMetaInstanceID']
        s2 = set(df_new_storage_id)
        self.assertTrue(s2.issubset(s1))

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckUpdateODKLastRun(unittest.TestCase):
    """Test methods that updates ODK_Conf.odk_last_run"""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            create_transfer_db('Pipeline.db', '.', 'enilepiP')
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()

        cls.copy_xfer_db = TransferDB(db_file_name='copy_Pipeline.db',
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db.connect_db()
        cls.settings_odk = cls.copy_xfer_db.config_odk(cls.copy_conn)
        cls.new_run_date = '3000-01-01_00:00:01'

    def test_method(self):
        """update_odk_last_run should change the date."""

        old_run_date = self.settings_odk.odk_last_run

        self.copy_xfer_db.update_odk_last_run(self.copy_conn,
                                              self.new_run_date)
        c = self.copy_conn.cursor()
        sql = 'SELECT odkLastRun FROM ODK_Conf'
        c.execute(sql)
        sql_query = c.fetchall()
        for i in sql_query:
            updated_run_date = i[0]
        self.assertEqual(updated_run_date, self.new_run_date)
        self.copy_xfer_db.update_odk_last_run(self.copy_conn, old_run_date)

    @classmethod
    def tearDownClass(cls):
        os.remove('Pipeline.db')


class CheckUpdateTableConf(unittest.TestCase):
    """Test methods for updated configuration tables (and their getters)."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile('test_conf_pipeline.db'):
            create_transfer_db('test_conf_pipeline.db', '.', 'enilepiP')
        db_file_name = 'test_conf_pipeline.db'
        db_key = 'enilepiP'
        db_directory = '.'
        pipeline_run_date = datetime.datetime.now()

        test_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        cls.conn = test_db.connect_db()

    def test_update_odk_conf(self):
        """should change values in ODK_Conf table"""
        list_fields = ['odkID', 'odkURL', 'odkUser', 'odkPassword',
                       'odkFormID', 'odkLastRun', 'odkUseCentral',
                       'odkProjectNumber']
        list_values = ['new_id', 'new_url', 'new_user', 'new_password',
                       'new_form_id', 'new_last_run', 'True',
                       'new_project_number']
        TransferDB.update_odk_conf(self.conn, list_fields, list_values)
        results = TransferDB.get_odk_conf(self.conn)
        for i in list_values:
            with self.subTest(i=i):
                self.assertTrue(i in results[0])

    def test_update_dhis_conf(self):
        """should change values in DHIS_Conf table"""
        list_fields = ['dhisURL', 'dhisUser', 'dhisPassword',
                       'dhisOrgUnit', 'dhisPostRoot']
        list_values = ['new_url', 'new_user', 'new_password',
                       'new_org_unit', 'True']
        TransferDB.update_dhis_conf(self.conn, list_fields, list_values)
        results = TransferDB.get_dhis_conf(self.conn)
        for i in list_values:
            with self.subTest(i=i):
                self.assertTrue(i in results[0])

    def test_update_pipeline_conf(self):
        """should change values in Pipeline_Conf table"""
        list_fields = ['algorithmMetadataCode', 'codSource', 'algorithm',
                       'workingDirectory']
        list_values = ['new_amc', 'ICD10', 'InSilicoVA',
                       'new_working_directory']
        TransferDB.update_pipeline_conf(self.conn, list_fields, list_values)
        results = TransferDB.get_pipeline_conf(self.conn)
        for i in list_values:
            with self.subTest(i=i):
                self.assertTrue(i in results[0])

    def test_get_tables(self):
        """should get all of the table names in transfer db"""
        table_names = ['Pipeline_Conf', 'VA_Storage', 'EventLog', 'ODK_Conf',
                       'InterVA_Conf', 'Advanced_InterVA_Conf',
                       'InSilicoVA_Conf', 'Advanced_InSilicoVA_Conf',
                       'SmartVA_Conf', 'SmartVA_Country', 'DHIS_Conf',
                       'COD_Codes_DHIS', 'Algorithm_Metadata_Options']
        results = TransferDB.get_tables(self.conn)
        for i in table_names:
            with self.subTest(i=i):
                self.assertTrue(i in results)

    def test_get_fields(self):
        """should get all of the fields in a table"""
        odk_fields = ['odkID', 'odkURL', 'odkUser', 'odkPassword',
                      'odkFormID', 'odkLastRun', 'odkUseCentral',
                      'odkProjectNumber']
        results = TransferDB.get_fields(self.conn, 'ODK_Conf')
        results_field_names = [i[0] for i in results]
        for i in odk_fields:
            with self.subTest(i=i):
                self.assertTrue(i in results_field_names)

    # get_schema returns an object that is pretty ugly (i.e. hard to test)
    # it does include any checks in the sql, so probably worth keeping
    # def test_get_schema(self):
    #     pass

    @classmethod
    def tearDownClass(cls):
        os.remove('test_conf_pipeline.db')


if __name__ == '__main__':
    unittest.main(verbosity=2)
