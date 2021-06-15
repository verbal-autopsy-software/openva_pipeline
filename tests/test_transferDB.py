import unittest
import os
import sqlite3
import shutil
import datetime
from pandas import read_csv
from pysqlcipher3 import dbapi2 as sqlcipher

from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context
from openva_pipeline.transferDB import TransferDB
from openva_pipeline.runPipeline import createTransferDB
from openva_pipeline.exceptions import DatabaseConnectionError
from openva_pipeline.exceptions import PipelineConfigurationError
from openva_pipeline.exceptions import ODKConfigurationError
from openva_pipeline.exceptions import OpenVAConfigurationError
from openva_pipeline.exceptions import DHISConfigurationError


os.chdir(os.path.abspath(os.path.dirname(__file__)))


class Check_DB_Has_Tables(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        pipelineRunDate = datetime.datetime.now()
        xferDB = TransferDB(dbFileName = 'Pipeline.db', dbDirectory = '.',
                            dbKey = 'enilepiP', plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sqlTestConnection = "SELECT name FROM SQLITE_MASTER where type = 'table';"
        c.execute(sqlTestConnection)
        tableNames = c.fetchall()
        cls.tableNames = tableNames
        cls.listTableNames = [j for i in tableNames for j in i]

    def test_connection_has_tables(self):
        """Test that the pipeline can connect to the DB."""
        self.assertTrue(len(self.tableNames) > 0)

    def test_table_Pipeline_Conf(self):
        """Test that the Pipeline.db has Pipeline_Conf table."""
        self.assertIn('Pipeline_Conf', self.listTableNames)

    def test_table_VA_Storage(self):
        """Test that the Pipeline.db has VA_Storage table."""
        self.assertIn('VA_Storage', self.listTableNames)

    def test_table_EventLog(self):
        """Test that the Pipeline.db has EventLog table."""
        self.assertIn('EventLog', self.listTableNames)

    def test_table_ODK_Conf(self):
        """Test that the Pipeline.db has ODK_Conf table."""
        self.assertIn('ODK_Conf', self.listTableNames)

    def test_table_InterVA_Conf(self):
        """Test that the Pipeline.db has InterVA_Conf table."""
        self.assertIn('InterVA_Conf', self.listTableNames)

    def test_table_Advanced_InterVA_Conf(self):
        """Test that the Pipeline.db has Advanced_InterVA_Conf table."""
        self.assertIn('Advanced_InterVA_Conf', self.listTableNames)

    def test_table_InSilicoVA_Conf(self):
        """Test that the Pipeline.db has InSilicoVA_Conf table."""
        self.assertIn('InSilicoVA_Conf', self.listTableNames)

    def test_table_Advanced_InSilicoVA_Conf(self):
        """Test that the Pipeline.db has Advanced_InSilicoVA_Conf table."""
        self.assertIn('Advanced_InSilicoVA_Conf', self.listTableNames)

    def test_table_SmartVA_Conf(self):
        """Test that the Pipeline.db has SmartVA_Conf table."""
        self.assertIn('SmartVA_Conf', self.listTableNames)

    def test_table_SmartVA_Country(self):
        """Test that the Pipeline.db has SmartVA_Country table."""
        self.assertIn('SmartVA_Country', self.listTableNames)

    def test_table_DHIS_Conf(self):
        """Test that the Pipeline.db has DHIS_Conf table."""
        self.assertIn('DHIS_Conf', self.listTableNames)

    def test_table_COD_Codes_DHIS(self):
        """Test that the Pipeline.db has COD_Codes_DHIS table."""
        self.assertIn('COD_Codes_DHIS', self.listTableNames)

    def test_table_Algorithm_Metadata_Options(self):
        """Test that the Pipeline.db has Algorithm_Metadata_Options table."""
        self.assertIn('Algorithm_Metadata_Options', self.listTableNames)

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_DB_Connection_Exceptions(unittest.TestCase):


    def test_dbFilePresent_exception(self):
        """Check that DB file is located in path."""

        badPath = '/invalid/path/to/pipelineDB'
        pipelineRunDate = datetime.datetime.now()
        xferDB = TransferDB(dbFileName = 'Pipeline.db', dbDirectory = badPath,
                            dbKey = 'enilepiP', plRunDate = pipelineRunDate)
        self.assertRaises(DatabaseConnectionError, xferDB.connectDB)

    def test_wrongKey_exception(self):
        """Pipeline should raise an error when the wrong key is used."""

        pipelineRunDate = datetime.datetime.now()
        xferDB = TransferDB(dbFileName = 'Pipeline.db', dbDirectory = '.',
                            dbKey = 'wrong_dbKey', plRunDate = pipelineRunDate)
        self.assertRaises(DatabaseConnectionError, xferDB.connectDB)


class Check_Pipeline_Conf(unittest.TestCase):
    """Test methods that grab configuration settings for pipline."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        dbDirectory = '.'
        pipelineRunDate = datetime.datetime.now()

        cls.xferDB = TransferDB(dbFileName = dbFileName,
                                dbDirectory = dbDirectory,
                                dbKey = dbKey,
                                plRunDate = pipelineRunDate)
        cls.conn = cls.xferDB.connectDB()
        c = cls.conn.cursor()
        c.execute('SELECT dhisCode from Algorithm_Metadata_Options;')
        cls.metadataQuery = c.fetchall()
        cls.settingsPipeline = cls.xferDB.configPipeline(cls.conn)

        cls.copy_xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                                     dbDirectory = dbDirectory,
                                     dbKey = dbKey,
                                     plRunDate = pipelineRunDate)
        cls.copy_conn = cls.copy_xferDB.connectDB()

        # parameters for connecting to DB with wrong Tables
        wrongTables_dbFileName = 'wrongTables_Pipeline.db'
        cls.wrongTables_xferDB = TransferDB(dbFileName = wrongTables_dbFileName,
                                            dbDirectory = dbDirectory,
                                            dbKey = dbKey,
                                            plRunDate = pipelineRunDate)
        cls.wrongTables_conn = cls.wrongTables_xferDB.connectDB()
        cls.wrongTables_c = cls.wrongTables_conn.cursor()

        # parameters for connecting to DB with wrong fields
        wrongFields_dbFileName = 'wrongFields_Pipeline.db'
        cls.wrongFields_xferDB = TransferDB(dbFileName = wrongFields_dbFileName,
                                            dbDirectory = dbDirectory,
                                            dbKey = dbKey,
                                            plRunDate = pipelineRunDate)
        cls.wrongFields_conn = cls.wrongFields_xferDB.connectDB()

    def test_pipelineConf_Exception_noTable(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(PipelineConfigurationError,
                          self.wrongTables_xferDB.configPipeline,
                          self.wrongTables_conn
        )

    def test_pipelineConf_Exception_noField(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(PipelineConfigurationError,
                          self.wrongFields_xferDB.configPipeline,
                          self.wrongFields_conn
        )

    # Thinking about removing this check -- 2021-06-09 (jt)
    # def test_pipelineConf_algorithmMetadataCode(self):
    #     """Test Pipeline_Conf table has valid algorithmMetadataCode."""

    #     validMetadataCode = self.settingsPipeline.algorithmMetadataCode in \
    #         [j for i in self.metadataQuery for j in i]
    #     self.assertTrue(validMetadataCode)

    # def test_pipelineConf_algorithmMetadataCode_Exception_value(self):
    #     """configPipeline should fail with invalid algorithmMetadataCode."""

    #     c = self.copy_conn.cursor()
    #     sql = 'UPDATE Pipeline_Conf SET algorithmMetadataCode = ?'
    #     par = ('wrong',)
    #     c.execute(sql, par)
    #     self.assertRaises(PipelineConfigurationError,
    #                       self.copy_xferDB.configPipeline, self.copy_conn)
    #     self.copy_conn.rollback()

    def test_pipelineConf_codSource(self):
        """Test Pipeline_Conf table has valid codSource"""

        validcodSource = self.settingsPipeline.codSource in \
            ('ICD10', 'WHO', 'Tariff')
        self.assertTrue(validcodSource)
    def test_pipelineConf_codSource_Exception(self):
        """configPipeline should fail with invalid codSource."""

        c = self.copy_conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET codSource = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xferDB.configPipeline, self.copy_conn)
        self.copy_conn.rollback()

    def test_pipelineConf_algorithm(self):
        """Test Pipeline_Conf table has valid algorithm"""

        validAlgorithm = self.settingsPipeline.algorithm in \
            ('InSilicoVA', 'InterVA', 'SmartVA')

        self.assertTrue(validAlgorithm)
    def test_pipelineConf_algorithm_Exception(self):
        """configPipeline should fail with invalid algorithm."""

        c = self.copy_conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xferDB.configPipeline, self.copy_conn)
        self.copy_conn.rollback()

    def test_pipelineConf_workingDirectory(self):
        """Test Pipeline_Conf table has valide algorithm"""

        validWD = os.path.isdir(self.settingsPipeline.workingDirectory)

        self.assertTrue(validWD)
    def test_pipelineConf_workingDirectory_Exception(self):
        """configPipeline should fail with invalid workingDirectory."""

        c = self.copy_conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET workingDirectory = ?'
        par = ('/wrong/path',)
        c.execute(sql, par)
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xferDB.configPipeline, self.copy_conn)
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_ODK_Conf(unittest.TestCase):
    """Test methods that grab ODK configuration."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        dbDirectory = '.'
        pipelineRunDate = datetime.datetime.now()

        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        cls.settingsODK = xferDB.configODK(conn)

        cls.copy_xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                                     dbDirectory = dbDirectory,
                                     dbKey = dbKey,
                                     plRunDate = pipelineRunDate)
        cls.copy_conn = cls.copy_xferDB.connectDB()

    def test_odkConf_odkURL(self):
        """Test ODK_Conf table has valid odkURL"""
        self.assertEqual(self.settingsODK.odkURL,
                         'https://odk.swisstph.ch/ODKAggregateOpenVa')
    def test_odkConf_odkURL_Exception(self):
        """configODK should fail with invalid url."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE ODK_Conf SET odkURL = ?'
        par = ('wrong.url',)
        c.execute(sql, par)
        self.assertRaises(ODKConfigurationError,
                          self.copy_xferDB.configODK, self.copy_conn)
        self.copy_conn.rollback()

    def test_odkConf_odkUser(self):
        """Test ODK_Conf table has valid odkUser"""
        self.assertEqual(self.settingsODK.odkUser, 'odk_openva')

    def test_odkConf_odkPassword(self):
        """Test ODK_Conf table has valid odkPassword"""
        self.assertEqual(self.settingsODK.odkPassword, 'openVA2018')

    def test_odkConf_odkFormID(self):
        """Test ODK_Conf table has valid odkFormID"""
        self.assertEqual(self.settingsODK.odkFormID, 'va_who_v1_5_1')

    def test_odkConf_odkLastRun(self):
        """Test ODK_Conf table has valid odkLastRun"""
        self.assertEqual(self.settingsODK.odkLastRun, '1900-01-01_00:00:01')

    def test_odkConf_odkLastRunDate(self):
        """Test ODK_Conf table has valid odkLastRunDate"""
        self.assertEqual(self.settingsODK.odkLastRunDate, '1900/01/01')

    def test_odkConf_odkLastRunDatePrev(self):
        """Test ODK_Conf table has valid odkLastRunDatePrev"""
        self.assertEqual(self.settingsODK.odkLastRunDatePrev, '1899/12/31')

    def test_odkConf_odkUseCentral(self):
        """Test ODK_Conf table has valid odkUseCentral"""
        self.assertEqual(self.settingsODK.odkUseCentral, 'False')

    def test_odkConf_odkProjectNumber(self):
        """Test ODK_Conf table has valid odkProjectNumber"""
        self.assertEqual(self.settingsODK.odkProjectNumber, '40')

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_OpenVA_Conf_InterVA(unittest.TestCase):
    """Test methods that grab InterVA configuration."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        dbDirectory = '.'
        pipelineRunDate = datetime.datetime.now()

        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        cls.settingsPipeline = xferDB.configPipeline(conn)
        cls.settingsOpenVA = xferDB.configOpenVA(conn,
                                                 'InterVA',
                                                 cls.settingsPipeline.workingDirectory)

        cls.copy_xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                                     dbDirectory = dbDirectory,
                                     dbKey = dbKey,
                                     plRunDate = pipelineRunDate)
        cls.copy_conn = cls.copy_xferDB.connectDB()

    def test_openvaConf_InterVA_Version(self):
        """Test InterVA_Conf table has valid version"""
        self.assertIn(self.settingsOpenVA.InterVA_Version, ('4', '5'))
    def test_openvaConf_InterVA_Version_Exception(self):
        """configOpenVA should fail with invalid InterVA_Conf.Version value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InterVA_Conf SET version = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_HIV(self):
        """Test InterVA_Conf table has valid HIV"""
        self.assertIn(self.settingsOpenVA.InterVA_HIV, ('v', 'l', 'h'))
    def test_openvaConf_InterVA_HIV_Exception(self):
        """configOpenVA should fail with invalid InterVA_Conf.HIV value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InterVA_Conf SET HIV = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_Malaria(self):
        """Test InterVA_Conf table has valid Malaria"""
        self.assertIn(self.settingsOpenVA.InterVA_Malaria, ('v', 'l', 'h'))
    def test_openvaConf_InterVA_Malaria_Exception(self):
        """configOpenVA should fail with invalid InterVA_Conf.Malaria value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InterVA_Conf SET Malaria = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_output(self):
        """Test Advanced_InterVA_Conf table has valid output."""
        self.assertIn(self.settingsOpenVA.InterVA_output,
                      ('classic', 'extended')
        )
    def test_openvaConf_InterVA_output_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.output value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET output = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_append(self):
        """Test Advanced_InterVA_Conf table has valid append."""
        self.assertIn(self.settingsOpenVA.InterVA_append,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InterVA_append_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.append value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET append = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_groupcode(self):
        """Test Advanced_InterVA_Conf table has valid groupcode."""
        self.assertIn(self.settingsOpenVA.InterVA_groupcode,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InterVA_groupcode_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.groupcode value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET groupcode = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_replicate(self):
        """Test Advanced_InterVA_Conf table has valid replicate."""
        self.assertIn(self.settingsOpenVA.InterVA_replicate,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InterVA_replicate_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.replicate value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET replicate = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_replicate_bug1(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug1."""
        self.assertIn(self.settingsOpenVA.InterVA_replicate_bug1,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InterVA_replicate_bug1_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.replicate_bug1 value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET replicate_bug1 = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InterVA_replicate_bug2(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug2."""
        self.assertIn(self.settingsOpenVA.InterVA_replicate_bug2,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InterVA_replicate_bug2_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.replicate_bug2 value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InterVA_Conf SET replicate_bug2 = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InterVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_OpenVA_Conf_InSilicoVA(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        dbDirectory = '.'
        pipelineRunDate = datetime.datetime.now()

        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        cls.settingsPipeline = xferDB.configPipeline(conn)
        cls.settingsOpenVA = xferDB.configOpenVA(conn,
                                                 'InSilicoVA',
                                                 cls.settingsPipeline.workingDirectory)

        cls.copy_xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                                     dbDirectory = dbDirectory,
                                     dbKey = dbKey,
                                     plRunDate = pipelineRunDate)
        cls.copy_conn = cls.copy_xferDB.connectDB()

    def test_openvaConf_InSilicoVA_data_type(self):
        """Test InSilicoVA_Conf table has valid data_type"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_data_type,
                      ('WHO2012', 'WHO2016')
        )
    def test_openvaConf_InSilicoVA_data_type_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.data_type value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InSilicoVA_Conf SET data_type = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_Nsim(self):
        """Test InSilicoVA_Conf table has valid Nsim"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_Nsim, '4000')
    def test_openvaConf_InSilicoVA_Nsim_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.Nsim value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE InSilicoVA_Conf SET Nsim = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_isNumeric(self):
        """Test InSilicoVA_Conf table has valid isNumeric"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_isNumeric,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_isNumeric_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.isNumeric value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET isNumeric = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_updateCondProb(self):
        """Test InSilicoVA_Conf table has valid updateCondProb"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_updateCondProb,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_updateCondProb_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.updateCondProb value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET updateCondProb = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_keepProbbase_level(self):
        """Test InSilicoVA_Conf table has valid keepProbbase_level"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_keepProbbase_level,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_keepProbbase_level_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.keepProbbase_level value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET keepProbbase_level = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_CondProb(self):
        """Test InSilicoVA_Conf table has valid CondProb"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_CondProb, 'NULL')
    def test_openvaConf_InSilicoVA_CondProb_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.CondProb value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET CondProb = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_CondProbNum(self):
        """Test InSilicoVA_Conf table has valid CondProbNum"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_CondProbNum, 'NULL')
    def test_openvaConf_InSilicoVA_CondProbNum2(self):
        """configOpenVA should accept InSilicoVA_Conf.CondProbNum '.35'."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET CondProbNum = ?'
        par = ('.35',)
        c.execute(sql, par)
        newTest = self.copy_xferDB.configOpenVA(self.copy_conn,
                                                'InSilicoVA',
                                                self.settingsPipeline.workingDirectory)
        self.assertEqual(newTest.InSilicoVA_CondProbNum, '.35')
        self.copy_conn.rollback()
    def test_openvaConf_InSilicoVA_CondProbNum_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.CondProbNum value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET CondProbNum = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_datacheck(self):
        """Test InSilicoVA_Conf table has valid datacheck"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_datacheck,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_datacheck_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.datacheck value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET datacheck = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_datacheck_missing(self):
        """Test InSilicoVA_Conf table has valid datacheck_missing"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_datacheck_missing,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_datacheck_missing_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.datacheck_missing value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET datacheck_missing = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_external_sep(self):
        """Test InSilicoVA_Conf table has valid external_sep"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_external_sep,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_external_sep_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.external_sep value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET external_sep = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_thin(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_thin, '10')
    def test_openvaConf_InSilicoVA_thin_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.thin value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET thin = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_burnin(self):
        """Test InSilicoVA_Conf table has valid burnin"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_burnin, '2000')
    def test_openvaConf_InSilicoVA_burnin_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.burnin value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET burnin = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_auto_length(self):
        """Test InSilicoVA_Conf table has valid auto_length"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_auto_length,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_auto_length_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.auto_length value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET auto_length = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_conv_csmf(self):
        """Test InSilicoVA_Conf table has valid conv_csmf"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_conv_csmf, '0.02')
    def test_openvaConf_InSilicoVA_conv_csmf_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.conv_csmf value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET conv_csmf = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_jump_scale(self):
        """Test InSilicoVA_Conf table has valid jump_scale"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_jump_scale, '0.1')
    def test_openvaConf_InSilicoVA_jump_scale_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.jump_scale value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET jump_scale = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_levels_prior(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_levels_prior, 'NULL')
    def test_openvaConf_InSilicoVA_levels_prior_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.levels_prior value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET levels_prior = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_levels_strength(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_levels_strength, '1')
    def test_openvaConf_InSilicoVA_levels_strength_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.levels_strength value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET levels_strength = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_trunc_min(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_trunc_min, '0.0001')
    def test_openvaConf_InSilicoVA_trunc_min_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.trunc_min value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET trunc_min = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_trunc_max(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_trunc_max, '0.9999')
    def test_openvaConf_InSilicoVA_trunc_max_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.trunc_max value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET trunc_max = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_subpop(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_subpop, 'NULL')
    def test_openvaConf_InSilicoVA_subpop_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.subpop value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET subpop = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_java_option(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_java_option, '-Xmx1g')
    def test_openvaConf_InSilicoVA_java_option_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.java_option value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET java_option = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_seed(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_seed, '1')
    def test_openvaConf_InSilicoVA_seed_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.seed value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET seed = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_phy_code(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_code, 'NULL')
    def test_openvaConf_InSilicoVA_phy_code_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_code value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_code = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_phy_cat(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_cat, 'NULL')
    def test_openvaConf_InSilicoVA_phy_cat_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_cat value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_cat = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_phy_unknown(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_unknown, 'NULL')
    def test_openvaConf_InSilicoVA_phy_unknown_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_unknown value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_unknown = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_phy_external(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_external, 'NULL')
    def test_openvaConf_InSilicoVA_phy_external_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_external value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_external = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_phy_debias(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_debias, 'NULL')
    def test_openvaConf_InSilicoVA_phy_debias_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_debias value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET phy_debias = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_exclude_impossible_cause(self):
        self.assertIn(self.settingsOpenVA.InSilicoVA_exclude_impossible_cause,
                      ('subset', 'all', 'InterVA', 'none')
        )
    def test_openvaConf_InSilicoVA_exclude_impossible_cause_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.exclude_impossible_cause value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET exclude_impossible_cause = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_no_is_missing(self):
        self.assertIn(self.settingsOpenVA.InSilicoVA_no_is_missing,
                      ('TRUE', 'FALSE')
        )
    def test_openvaConf_InSilicoVA_no_is_missing_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.no_is_missing value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET no_is_missing = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_indiv_CI(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_indiv_CI, 'NULL')
    def test_openvaConf_InSilicoVA_indiv_CI_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.indiv_CI value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET indiv_CI = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_openvaConf_InSilicoVA_groupcode(self):
        self.assertIn(self.settingsOpenVA.InSilicoVA_groupcode, ('TRUE', 'FALSE'))
    def test_openvaConf_InSilicoVA_groupcode_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.groupcode value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE Advanced_InSilicoVA_Conf SET groupcode = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'InSilicoVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_SmartVA_Conf(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        dbDirectory = '.'
        pipelineRunDate = datetime.datetime.now()

        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        cls.settingsPipeline = xferDB.configPipeline(conn)
        cls.settingsSmartva = xferDB.configOpenVA(conn,
                                                  'SmartVA',
                                                  cls.settingsPipeline.workingDirectory)

        cls.copy_xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                                     dbDirectory = dbDirectory,
                                     dbKey = dbKey,
                                     plRunDate = pipelineRunDate)
        cls.copy_conn = cls.copy_xferDB.connectDB()

    def test_smartvaConf_country(self):
        """Test SmartVA_Conf table has valid country"""
        self.assertEqual(self.settingsSmartva.SmartVA_country, 'Unknown')
    def test_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.country value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET country = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'SmartVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_smartvaConf_hiv(self):
        """Test SmartVA_Conf table has valid hiv"""
        self.assertEqual(self.settingsSmartva.SmartVA_hiv, 'False')
    def test_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.hiv value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET hiv = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'SmartVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_smartvaConf_malaria(self):
        """Test SmartVA_Conf table has valid malaria"""
        self.assertEqual(self.settingsSmartva.SmartVA_malaria, 'False')
    def test_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.malaria value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET malaria = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'SmartVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_smartvaConf_hce(self):
        """Test SmartVA_Conf table has valid hce"""
        self.assertEqual(self.settingsSmartva.SmartVA_hce, 'False')
    def test_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.hce value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET hce = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'SmartVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_smartvaConf_freetext(self):
        """Test SmartVA_Conf table has valid freetext"""
        self.assertEqual(self.settingsSmartva.SmartVA_freetext, 'False')
    def test_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.freetext value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET freetext = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'SmartVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_smartvaConf_figures(self):
        """Test SmartVA_Conf table has valid figures"""
        self.assertEqual(self.settingsSmartva.SmartVA_figures, 'False')
    def test_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.figures value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET figures = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'SmartVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_smartvaConf_language(self):
        """Test SmartVA_Conf table has valid language"""
        self.assertEqual(self.settingsSmartva.SmartVA_language, 'english')
    def test_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.language value."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE SmartVA_Conf SET language = ?'
        par = ('wrong',)
        c.execute(sql, par)
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, 'SmartVA',
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_DHIS_Conf(unittest.TestCase):
    """Test methods that grab DHIS configuration."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        dbDirectory = '.'
        pipelineRunDate = datetime.datetime.now()
        cls.algorithm = 'InterVA'

        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        cls.settingsDHIS = xferDB.configDHIS(conn, cls.algorithm)

        cls.copy_xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                                     dbDirectory = dbDirectory,
                                     dbKey = dbKey,
                                     plRunDate = pipelineRunDate)
        cls.copy_conn = cls.copy_xferDB.connectDB()

    def test_dhisConf_dhisURL(self):
        """Test DHIS_Conf table has valid dhisURL"""
        self.assertEqual(self.settingsDHIS[0].dhisURL,
                         'https://va30se.swisstph-mis.ch')
    def test_dhisConf_dhisURL_Exception(self):
        """configDHIS should fail with invalid url."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisURL = ?'
        par = ('wrong.url',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_dhisConf_dhisUser(self):
        """Test DHIS_Conf table has valid dhisUser"""
        self.assertEqual(self.settingsDHIS[0].dhisUser, 'va-demo')
    def test_dhisConf_dhisUser_Exception(self):
        """configDHIS should fail with invalid dhisUser."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisUser = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_dhisConf_dhisPassword(self):
        """Test DHIS_Conf table has valid dhisPassword"""
        self.assertEqual(self.settingsDHIS[0].dhisPassword, 'VerbalAutopsy99!')
    def test_dhisConf_dhisPassword_Exception(self):
        """configDHIS should fail with invalid dhisPassword."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisPassword = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_dhisConf_dhisOrgUnit(self):
        """Test DHIS_Conf table has valid dhisOrgUnit"""
        self.assertEqual(self.settingsDHIS[0].dhisOrgUnit, 'SCVeBskgiK6')
    def test_dhisConf_dhisOrgUnit_Exception(self):
        """configDHIS should fail with invalid dhisOrgUnit."""
        c = self.copy_conn.cursor()
        sql = 'UPDATE DHIS_Conf SET dhisOrgUnit = ?'
        par = ('',)
        c.execute(sql, par)
        self.assertRaises(DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_DHIS_storeVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        shutil.copy('OpenVAFiles/sample_newStorage.csv',
                    'OpenVAFiles/newStorage.csv')

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        pipelineRunDate = datetime.datetime.now()
        wrong_dbKey = 'wrongKey'
        dbDirectory = '.'

        cls.xferDB = TransferDB(dbFileName = dbFileName,
                                dbDirectory = dbDirectory,
                                dbKey = dbKey,
                                plRunDate = pipelineRunDate)
        cls.conn = cls.xferDB.connectDB()
        cls.xferDB.configPipeline(cls.conn)

    def test_DHIS_storeVA(self):
        """Check that VA records get stored in Transfer DB."""

        self.xferDB.storeVA(self.conn)
        c = self.conn.cursor()
        sql = 'SELECT id FROM VA_Storage'
        c.execute(sql)
        vaIDs = c.fetchall()
        vaIDsList = [j for i in vaIDs for j in i]
        s1 = set(vaIDsList)
        dfNewStorage = read_csv('OpenVAFiles/newStorage.csv')
        dfNewStorageID = dfNewStorage['odkMetaInstanceID']
        s2 = set(dfNewStorageID)
        self.assertTrue(s2.issubset(s1))

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_updateODKLastRun(unittest.TestCase):
    """Test methods that updates ODK_Conf.odkLastRun"""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        dbDirectory = '.'
        pipelineRunDate = datetime.datetime.now()
        algorithm = 'InterVA'

        cls.copy_xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                                     dbDirectory = dbDirectory,
                                     dbKey = dbKey,
                                     plRunDate = pipelineRunDate)
        cls.copy_conn = cls.copy_xferDB.connectDB()
        cls.settingsODK = cls.copy_xferDB.configODK(cls.copy_conn)
        cls.newRunDate = '3000-01-01_00:00:01'

    def test_method(self):
        """updateODKLastRun should change the date."""

        oldRunDate = self.settingsODK.odkLastRun

        self.copy_xferDB.updateODKLastRun(self.copy_conn, self.newRunDate)
        c = self.copy_conn.cursor()
        sql = 'SELECT odkLastRun FROM ODK_Conf'
        c.execute(sql)
        sqlQuery = c.fetchall()
        for i in sqlQuery:
            updatedRunDate = i[0]
        self.assertEqual(updatedRunDate, self.newRunDate)
        self.copy_xferDB.updateODKLastRun(self.copy_conn, oldRunDate)

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


if __name__ == '__main__':
    unittest.main(verbosity = 2)
