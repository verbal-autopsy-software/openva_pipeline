#------------------------------------------------------------------------------#
# test_transferDB.py
#
# New tests:
#
# (1) Should you include an error if there are not enough arguments provided?
#
#------------------------------------------------------------------------------#

import unittest
import os
import sqlite3
import shutil
import datetime
from pandas import read_csv
from context import pipeline
from context import transferDB
from transferDB import TransferDB
from pysqlcipher3 import dbapi2 as sqlcipher

os.chdir(os.path.abspath(os.path.dirname(__file__)))

# Test valid DB connection & structure (i.e. all tables are there)
class Check_1_Connection(unittest.TestCase):

    # parameters for connecting to DB (assuming DB is in tests folder)
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    wrong_dbKey = "wrongKey"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    c = conn.cursor()
    sqlTestConnection = "SELECT name FROM SQLITE_MASTER where type = 'table';"
    c.execute(sqlTestConnection)
    tableNames = c.fetchall()
    listTableNames = [j for i in tableNames for j in i]

    def test_1_connection_1_dbFilePresent(self):
        """Check that DB file is located in path."""
        badPath = "/invalid/path/to/pipelineDB"
        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = badPath,
                            dbKey = self.dbKey,
                            plRunDate = self.pipelineRunDate)
        self.assertRaises(transferDB.DatabaseConnectionError, xferDB.connectDB)

    def test_1_connection_1_wrongKey(self):
        """Pipeline should raise an error when the wrong key is used."""
        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = self.dbDirectory,
                            dbKey = self.wrong_dbKey,
                            plRunDate = self.pipelineRunDate)
        self.assertRaises(transferDB.DatabaseConnectionError, xferDB.connectDB)

    def test_1_connection_2_connection(self):
        """Test that the pipeline can connect to the DB."""
        self.assertTrue(len(self.tableNames) > 0)

    def test_1_table_Pipeline_Conf(self):
        """Test that the Pipeline.db has Pipeline_Conf table."""
        self.assertIn("Pipeline_Conf", self.listTableNames)

    def test_1_table_VA_Storage(self):
        """Test that the Pipeline.db has VA_Storage table."""
        self.assertIn("VA_Storage", self.listTableNames)

    def test_1_table_EventLog(self):
        """Test that the Pipeline.db has EventLog table."""
        self.assertIn("EventLog", self.listTableNames)

    def test_1_table_ODK_Conf(self):
        """Test that the Pipeline.db has ODK_Conf table."""
        self.assertIn("ODK_Conf", self.listTableNames)

    def test_1_table_InterVA_Conf(self):
        """Test that the Pipeline.db has InterVA_Conf table."""
        self.assertIn("InterVA_Conf", self.listTableNames)

    def test_1_table_Advanced_InterVA_Conf(self):
        """Test that the Pipeline.db has Advanced_InterVA_Conf table."""
        self.assertIn("Advanced_InterVA_Conf", self.listTableNames)

    def test_1_table_InSilicoVA_Conf(self):
        """Test that the Pipeline.db has InSilicoVA_Conf table."""
        self.assertIn("InSilicoVA_Conf", self.listTableNames)

    def test_1_table_Advanced_InSilicoVA_Conf(self):
        """Test that the Pipeline.db has Advanced_InSilicoVA_Conf table."""
        self.assertIn("Advanced_InSilicoVA_Conf", self.listTableNames)

    def test_1_table_SmartVA_Conf(self):
        """Test that the Pipeline.db has SmartVA_Conf table."""
        self.assertIn("SmartVA_Conf", self.listTableNames)

    def test_1_table_SmartVA_Country(self):
        """Test that the Pipeline.db has SmartVA_Country table."""
        self.assertIn("SmartVA_Country", self.listTableNames)

    def test_1_table_DHIS_Conf(self):
        """Test that the Pipeline.db has DHIS_Conf table."""
        self.assertIn("DHIS_Conf", self.listTableNames)

    def test_1_table_COD_Codes_DHIS(self):
        """Test that the Pipeline.db has COD_Codes_DHIS table."""
        self.assertIn("COD_Codes_DHIS", self.listTableNames)

    def test_1_table_Algorithm_Metadata_Options(self):
        """Test that the Pipeline.db has Algorithm_Metadata_Options table."""
        self.assertIn("Algorithm_Metadata_Options", self.listTableNames)

# Test Pipeline Configuration
class Check_2_Pipeline_Conf(unittest.TestCase):
    """Test methods that grab configuration settings for pipline."""

    # parameters for connecting to DB (assuming DB is in tests folder)
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    c = conn.cursor()
    c.execute("SELECT dhisCode from Algorithm_Metadata_Options;")
    metadataQuery = c.fetchall()
    settingsPipeline = xferDB.configPipeline(conn)

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey,
                             plRunDate = pipelineRunDate)
    copy_conn = copy_xferDB.connectDB()

    # parameters for connecting to DB with wrong Tables
    wrongTables_dbFileName = "wrongTables_Pipeline.db"
    wrongTables_xferDB = TransferDB(dbFileName = wrongTables_dbFileName,
                                    dbDirectory = dbDirectory,
                                    dbKey = dbKey,
                                    plRunDate = pipelineRunDate)
    wrongTables_conn = wrongTables_xferDB.connectDB()
    wrongTables_c = wrongTables_conn.cursor()

    # parameters for connecting to DB with wrong fields
    wrongFields_dbFileName = "wrongFields_Pipeline.db"
    wrongFields_xferDB = TransferDB(dbFileName = wrongFields_dbFileName,
                                    dbDirectory = dbDirectory,
                                    dbKey = dbKey,
                                    plRunDate = pipelineRunDate)
    wrongFields_conn = wrongFields_xferDB.connectDB()

    def test_2_pipelineConf_Exception_noTable(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(transferDB.PipelineConfigurationError,
                          self.wrongTables_xferDB.configPipeline,
                          self.wrongTables_conn
        )
    def test_2_pipelineConf_Exception_noField(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(transferDB.PipelineConfigurationError,
                          self.wrongFields_xferDB.configPipeline,
                          self.wrongFields_conn
        )

    def test_2_pipelineConf_algorithmMetadataCode(self):
        """Test Pipeline_Conf table has valid algorithmMetadataCode."""

        validMetadataCode = self.settingsPipeline.algorithmMetadataCode in \
            [j for i in self.metadataQuery for j in i]
        self.assertTrue(validMetadataCode)

    def test_2_pipelineConf_algorithmMetadataCode_Exception_value(self):
        """configPipeline should fail with invalid algorithmMetadataCode."""

        c = self.copy_conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithmMetadataCode = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.PipelineConfigurationError,
                          self.copy_xferDB.configPipeline, self.copy_conn)
        self.copy_conn.rollback()

    def test_2_pipelineConf_codSource(self):
        """Test Pipeline_Conf table has valid codSource"""

        validcodSource = self.settingsPipeline.codSource in \
            ("ICD10", "WHO", "Tariff")
        self.assertTrue(validcodSource)
    def test_2_pipelineConf_codSource_Exception(self):
        """configPipeline should fail with invalid codSource."""

        c = self.copy_conn.cursor()
        sql = "UPDATE Pipeline_Conf SET codSource = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.PipelineConfigurationError,
                          self.copy_xferDB.configPipeline, self.copy_conn)
        self.copy_conn.rollback()

    def test_2_pipelineConf_algorithm(self):
        """Test Pipeline_Conf table has valid algorithm"""

        validAlgorithm = self.settingsPipeline.algorithm in \
            ("InSilicoVA", "InterVA", "SmartVA")

        self.assertTrue(validAlgorithm)
    def test_2_pipelineConf_algorithm_Exception(self):
        """configPipeline should fail with invalid algorithm."""

        c = self.copy_conn.cursor()
        sql = "UPDATE Pipeline_Conf SET algorithm = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.PipelineConfigurationError,
                          self.copy_xferDB.configPipeline, self.copy_conn)
        self.copy_conn.rollback()

    def test_2_pipelineConf_workingDirectory(self):
        """Test Pipeline_Conf table has valide algorithm"""

        validWD = os.path.isdir(self.settingsPipeline.workingDirectory)

        self.assertTrue(validWD)
    def test_2_pipelineConf_workingDirectory_Exception(self):
        """configPipeline should fail with invalid workingDirectory."""

        c = self.copy_conn.cursor()
        sql = "UPDATE Pipeline_Conf SET workingDirectory = ?"
        par = ("/wrong/path",)
        c.execute(sql, par)
        self.assertRaises(transferDB.PipelineConfigurationError,
                          self.copy_xferDB.configPipeline, self.copy_conn)
        self.copy_conn.rollback()

# Test ODK Configuration
class Check_3_ODK_Conf(unittest.TestCase):
    """Test methods that grab ODK configuration."""
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    settingsODK = xferDB.configODK(conn)

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey,
                             plRunDate = pipelineRunDate)
    copy_conn = copy_xferDB.connectDB()

    def test_3_odkConf_odkURL(self):
        """Test ODK_Conf table has valid odkURL"""
        self.assertEqual(self.settingsODK.odkURL,
                         "https://odk.swisstph.ch/ODKAggregateOpenVa")
    def test_3_odkConf_odkURL_Exception(self):
        """configODK should fail with invalid url."""
        c = self.copy_conn.cursor()
        sql = "UPDATE ODK_Conf SET odkURL = ?"
        par = ("wrong.url",)
        c.execute(sql, par)
        self.assertRaises(transferDB.ODKConfigurationError,
                          self.copy_xferDB.configODK, self.copy_conn)
        self.copy_conn.rollback()

    ## HERE: what if you don't need a username or password?
    def test_3_odkConf_odkUser(self):
        """Test ODK_Conf table has valid odkUser"""
        self.assertEqual(self.settingsODK.odkUser, "odk_openva")

    def test_3_odkConf_odkPassword(self):
        """Test ODK_Conf table has valid odkPassword"""
        self.assertEqual(self.settingsODK.odkPassword, "openVA2018")

    def test_3_odkConf_odkFormID(self):
        """Test ODK_Conf table has valid odkFormID"""
        self.assertEqual(self.settingsODK.odkFormID, "va_who_2016_11_03_v1_4_1")

    def test_3_odkConf_odkLastRun(self):
        """Test ODK_Conf table has valid odkLastRun"""
        self.assertEqual(self.settingsODK.odkLastRun, "1900-01-01_00:00:01")

    def test_3_odkConf_odkLastRunDate(self):
        """Test ODK_Conf table has valid odkLastRunDate"""
        self.assertEqual(self.settingsODK.odkLastRunDate, "1900/01/01")

    def test_3_odkConf_odkLastRunDatePrev(self):
        """Test ODK_Conf table has valid odkLastRunDatePrev"""
        self.assertEqual(self.settingsODK.odkLastRunDatePrev, "1899/12/31")

    # def test_3_odkConf_odkLastRunResult(self):
    #     """Test ODK_Conf table has valid odkLastRunResult"""
    #     self.assertIn(self.settingsODK.odkLastRunResult, ("success","fail"))
    # def test_3_odkConf_odkLastRunResult_Exception(self):
    #     """configODK should fail with invalid odkLastRunResult."""
    #     c = self.copy_conn.cursor()
    #     sql = "UPDATE ODK_Conf SET odkLastRunResult = ?"
    #     par = ("wrong",)
    #     c.execute(sql, par)
    #     self.assertRaises(transferDB.ODKConfigurationError,
    #                       self.copy_xferDB.configODK, self.copy_conn)
    #     self.copy_conn.rollback()    

# Test InterVA Configuration
class Check_4_OpenVA_Conf_InterVA(unittest.TestCase):
    """Test methods that grab InterVA configuration."""
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    settingsPipeline = xferDB.configPipeline(conn)
    settingsOpenVA = xferDB.configOpenVA(conn,
                                         "InterVA",
                                         settingsPipeline.workingDirectory)

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey,
                             plRunDate = pipelineRunDate)
    copy_conn = copy_xferDB.connectDB()

    def test_4_openvaConf_InterVA_Version(self):
        """Test InterVA_Conf table has valid version"""
        self.assertIn(self.settingsOpenVA.InterVA_Version, ("4", "5"))
    def test_4_openvaConf_InterVA_Version_Exception(self):
        """configOpenVA should fail with invalid InterVA_Conf.Version value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE InterVA_Conf SET version = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_HIV(self):
        """Test InterVA_Conf table has valid HIV"""
        self.assertIn(self.settingsOpenVA.InterVA_HIV, ("v", "l", "h"))
    def test_4_openvaConf_InterVA_HIV_Exception(self):
        """configOpenVA should fail with invalid InterVA_Conf.HIV value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE InterVA_Conf SET HIV = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_Malaria(self):
        """Test InterVA_Conf table has valid Malaria"""
        self.assertIn(self.settingsOpenVA.InterVA_Malaria, ("v", "l", "h"))
    def test_4_openvaConf_InterVA_Malaria_Exception(self):
        """configOpenVA should fail with invalid InterVA_Conf.Malaria value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE InterVA_Conf SET Malaria = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_output(self):
        """Test Advanced_InterVA_Conf table has valid output."""
        self.assertIn(self.settingsOpenVA.InterVA_output,
                      ("classic", "extended")
        )
    def test_4_openvaConf_InterVA_output_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.output value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InterVA_Conf SET output = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_append(self):
        """Test Advanced_InterVA_Conf table has valid append."""
        self.assertIn(self.settingsOpenVA.InterVA_append,
                      ("TRUE", "FALSE")
        )
    def test_4_openvaConf_InterVA_append_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.append value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InterVA_Conf SET append = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_groupcode(self):
        """Test Advanced_InterVA_Conf table has valid groupcode."""
        self.assertIn(self.settingsOpenVA.InterVA_groupcode,
                      ("TRUE", "FALSE")
        )
    def test_4_openvaConf_InterVA_groupcode_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.groupcode value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InterVA_Conf SET groupcode = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_replicate(self):
        """Test Advanced_InterVA_Conf table has valid replicate."""
        self.assertIn(self.settingsOpenVA.InterVA_replicate,
                      ("TRUE", "FALSE")
        )
    def test_4_openvaConf_InterVA_replicate_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.replicate value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InterVA_Conf SET replicate = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_replicate_bug1(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug1."""
        self.assertIn(self.settingsOpenVA.InterVA_replicate_bug1,
                      ("TRUE", "FALSE")
        )
    def test_4_openvaConf_InterVA_replicate_bug1_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.replicate_bug1 value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InterVA_Conf SET replicate_bug1 = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_4_openvaConf_InterVA_replicate_bug2(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug2."""
        self.assertIn(self.settingsOpenVA.InterVA_replicate_bug2,
                      ("TRUE", "FALSE")
        )
    def test_4_openvaConf_InterVA_replicate_bug2_Exception(self):
        """configOpenVA should fail with invalid Advanced_InterVA_Conf.replicate_bug2 value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InterVA_Conf SET replicate_bug2 = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InterVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

# Test InSilicoVA Configuration
class Check_5_OpenVA_Conf_InSilicoVA(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    settingsPipeline = xferDB.configPipeline(conn)
    settingsOpenVA = xferDB.configOpenVA(conn,
                                         "InSilicoVA",
                                         settingsPipeline.workingDirectory)

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey,
                             plRunDate = pipelineRunDate)
    copy_conn = copy_xferDB.connectDB()

    def test_5_openvaConf_InSilicoVA_data_type(self):
        """Test InSilicoVA_Conf table has valid data_type"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_data_type,
                      ("WHO2012", "WHO2016")
        )
    def test_5_openvaConf_InSilicoVA_data_type_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.data_type value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE InSilicoVA_Conf SET data_type = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_Nsim(self):
        """Test InSilicoVA_Conf table has valid Nsim"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_Nsim, "4000")
    def test_5_openvaConf_InSilicoVA_Nsim_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.Nsim value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE InSilicoVA_Conf SET Nsim = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_isNumeric(self):
        """Test InSilicoVA_Conf table has valid isNumeric"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_isNumeric,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_isNumeric_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.isNumeric value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET isNumeric = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_updateCondProb(self):
        """Test InSilicoVA_Conf table has valid updateCondProb"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_updateCondProb,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_updateCondProb_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.updateCondProb value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET updateCondProb = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_keepProbbase_level(self):
        """Test InSilicoVA_Conf table has valid keepProbbase_level"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_keepProbbase_level,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_keepProbbase_level_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.keepProbbase_level value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET keepProbbase_level = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_CondProb(self):
        """Test InSilicoVA_Conf table has valid CondProb"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_CondProb, "NULL")
    def test_5_openvaConf_InSilicoVA_CondProb_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.CondProb value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET CondProb = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_CondProbNum(self):
        """Test InSilicoVA_Conf table has valid CondProbNum"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_CondProbNum, "NULL")
    def test_5_openvaConf_InSilicoVA_CondProbNum2(self):
        """configOpenVA should accept InSilicoVA_Conf.CondProbNum '.35'."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET CondProbNum = ?"
        par = (".35",)
        c.execute(sql, par)
        newTest = self.copy_xferDB.configOpenVA(self.copy_conn,
                                                "InSilicoVA",
                                                self.settingsPipeline.workingDirectory)
        self.assertEqual(newTest.InSilicoVA_CondProbNum, ".35")
        self.copy_conn.rollback()
    def test_5_openvaConf_InSilicoVA_CondProbNum_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.CondProbNum value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET CondProbNum = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_datacheck(self):
        """Test InSilicoVA_Conf table has valid datacheck"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_datacheck,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_datacheck_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.datacheck value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET datacheck = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_datacheck_missing(self):
        """Test InSilicoVA_Conf table has valid datacheck_missing"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_datacheck_missing,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_datacheck_missing_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.datacheck_missing value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET datacheck_missing = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_external_sep(self):
        """Test InSilicoVA_Conf table has valid external_sep"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_external_sep,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_external_sep_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.external_sep value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET external_sep = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_thin(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_thin, "10")
    def test_5_openvaConf_InSilicoVA_thin_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.thin value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET thin = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_burnin(self):
        """Test InSilicoVA_Conf table has valid burnin"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_burnin, "2000")
    def test_5_openvaConf_InSilicoVA_burnin_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.burnin value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET burnin = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_auto_length(self):
        """Test InSilicoVA_Conf table has valid auto_length"""
        self.assertIn(self.settingsOpenVA.InSilicoVA_auto_length,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_auto_length_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.auto_length value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET auto_length = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_conv_csmf(self):
        """Test InSilicoVA_Conf table has valid conv_csmf"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_conv_csmf, "0.02")
    def test_5_openvaConf_InSilicoVA_conv_csmf_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.conv_csmf value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET conv_csmf = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_jump_scale(self):
        """Test InSilicoVA_Conf table has valid jump_scale"""
        self.assertEqual(self.settingsOpenVA.InSilicoVA_jump_scale, "0.1")
    def test_5_openvaConf_InSilicoVA_jump_scale_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.jump_scale value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET jump_scale = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_levels_prior(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_levels_prior, "NULL")
    def test_5_openvaConf_InSilicoVA_levels_prior_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.levels_prior value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET levels_prior = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_levels_strength(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_levels_strength, "1")
    def test_5_openvaConf_InSilicoVA_levels_strength_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.levels_strength value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET levels_strength = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_trunc_min(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_trunc_min, "0.0001")
    def test_5_openvaConf_InSilicoVA_trunc_min_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.trunc_min value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET trunc_min = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_trunc_max(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_trunc_max, "0.9999")
    def test_5_openvaConf_InSilicoVA_trunc_max_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.trunc_max value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET trunc_max = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_subpop(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_subpop, "NULL")
    def test_5_openvaConf_InSilicoVA_subpop_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.subpop value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET subpop = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_java_option(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_java_option, "-Xmx1g")
    def test_5_openvaConf_InSilicoVA_java_option_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.java_option value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET java_option = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_seed(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_seed, "1")
    def test_5_openvaConf_InSilicoVA_seed_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.seed value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET seed = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_phy_code(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_code, "NULL")
    def test_5_openvaConf_InSilicoVA_phy_code_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_code value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET phy_code = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_phy_cat(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_cat, "NULL")
    def test_5_openvaConf_InSilicoVA_phy_cat_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_cat value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET phy_cat = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_phy_unknown(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_unknown, "NULL")
    def test_5_openvaConf_InSilicoVA_phy_unknown_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_unknown value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET phy_unknown = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_phy_external(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_external, "NULL")
    def test_5_openvaConf_InSilicoVA_phy_external_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_external value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET phy_external = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_phy_debias(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_phy_debias, "NULL")
    def test_5_openvaConf_InSilicoVA_phy_debias_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.phy_debias value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET phy_debias = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_exclude_impossible_cause(self):
        self.assertIn(self.settingsOpenVA.InSilicoVA_exclude_impossible_cause,
                      ("subset", "all", "InterVA", "none")
        )
    def test_5_openvaConf_InSilicoVA_exclude_impossible_cause_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.exclude_impossible_cause value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET exclude_impossible_cause = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_no_is_missing(self):
        self.assertIn(self.settingsOpenVA.InSilicoVA_no_is_missing,
                      ("TRUE", "FALSE")
        )
    def test_5_openvaConf_InSilicoVA_no_is_missing_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.no_is_missing value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET no_is_missing = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_indiv_CI(self):
        self.assertEqual(self.settingsOpenVA.InSilicoVA_indiv_CI, "NULL")
    def test_5_openvaConf_InSilicoVA_indiv_CI_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.indiv_CI value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET indiv_CI = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_openvaConf_InSilicoVA_groupcode(self):
        self.assertIn(self.settingsOpenVA.InSilicoVA_groupcode, ("TRUE", "FALSE"))
    def test_5_openvaConf_InSilicoVA_groupcode_Exception(self):
        """configOpenVA should fail with invalid InSilicoVA_Conf.groupcode value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE Advanced_InSilicoVA_Conf SET groupcode = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "InSilicoVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

# Test SmartVA Configuration
class Check_5_SmartVA_Conf(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    settingsPipeline = xferDB.configPipeline(conn)
    settingsSmartva = xferDB.configOpenVA(conn,
                                          "SmartVA",
                                          settingsPipeline.workingDirectory)

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey,
                             plRunDate = pipelineRunDate)
    copy_conn = copy_xferDB.connectDB()

    def test_5_smartvaConf_country(self):
        """Test SmartVA_Conf table has valid country"""
        self.assertEqual(self.settingsSmartva.SmartVA_country, "Unknown")
    def test_5_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.country value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE SmartVA_Conf SET country = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "SmartVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_smartvaConf_hiv(self):
        """Test SmartVA_Conf table has valid hiv"""
        self.assertEqual(self.settingsSmartva.SmartVA_hiv, "False")
    def test_5_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.hiv value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE SmartVA_Conf SET hiv = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "SmartVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_smartvaConf_malaria(self):
        """Test SmartVA_Conf table has valid malaria"""
        self.assertEqual(self.settingsSmartva.SmartVA_malaria, "False")
    def test_5_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.malaria value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE SmartVA_Conf SET malaria = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "SmartVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_smartvaConf_hce(self):
        """Test SmartVA_Conf table has valid hce"""
        self.assertEqual(self.settingsSmartva.SmartVA_hce, "False")
    def test_5_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.hce value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE SmartVA_Conf SET hce = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "SmartVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_smartvaConf_freetext(self):
        """Test SmartVA_Conf table has valid freetext"""
        self.assertEqual(self.settingsSmartva.SmartVA_freetext, "False")
    def test_5_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.freetext value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE SmartVA_Conf SET freetext = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "SmartVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_smartvaConf_figures(self):
        """Test SmartVA_Conf table has valid figures"""
        self.assertEqual(self.settingsSmartva.SmartVA_figures, "False")
    def test_5_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.figures value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE SmartVA_Conf SET figures = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "SmartVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

    def test_5_smartvaConf_language(self):
        """Test SmartVA_Conf table has valid language"""
        self.assertEqual(self.settingsSmartva.SmartVA_language, "english")
    def test_5_smartvaConf_data_type_Exception(self):
        """configOpenVA should fail with invalid SmartVA_Conf.language value."""
        c = self.copy_conn.cursor()
        sql = "UPDATE SmartVA_Conf SET language = ?"
        par = ("wrong",)
        c.execute(sql, par)
        self.assertRaises(transferDB.OpenVAConfigurationError,
                          self.copy_xferDB.configOpenVA,
                          self.copy_conn, "SmartVA",
                          self.settingsPipeline.workingDirectory)
        self.copy_conn.rollback()

# Test DHIS Configuration
class Check_6_DHIS_Conf(unittest.TestCase):
    """Test methods that grab DHIS configuration."""
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()
    algorithm = "InterVA"

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    settingsDHIS = xferDB.configDHIS(conn, algorithm)

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey,
                             plRunDate = pipelineRunDate)
    copy_conn = copy_xferDB.connectDB()

    def test_6_dhisConf_dhisURL(self):
        """Test DHIS_Conf table has valid dhisURL"""        
        self.assertEqual(self.settingsDHIS[0].dhisURL,
                         "https://va30se.swisstph-mis.ch")
    def test_6_dhisConf_dhisURL_Exception(self):
        """configDHIS should fail with invalid url."""
        c = self.copy_conn.cursor()
        sql = "UPDATE DHIS_Conf SET dhisURL = ?"
        par = ("wrong.url",)
        c.execute(sql, par)
        self.assertRaises(transferDB.DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_6_dhisConf_dhisUser(self):
        """Test DHIS_Conf table has valid dhisUser"""
        self.assertEqual(self.settingsDHIS[0].dhisUser, "va-demo")
    def test_6_dhisConf_dhisUser_Exception(self):
        """configDHIS should fail with invalid dhisUser."""
        c = self.copy_conn.cursor()
        sql = "UPDATE DHIS_Conf SET dhisUser = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_6_dhisConf_dhisPassword(self):
        """Test DHIS_Conf table has valid dhisPassword"""
        self.assertEqual(self.settingsDHIS[0].dhisPassword, "VerbalAutopsy99!")
    def test_6_dhisConf_dhisPassword_Exception(self):
        """configDHIS should fail with invalid dhisPassword."""
        c = self.copy_conn.cursor()
        sql = "UPDATE DHIS_Conf SET dhisPassword = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

    def test_6_dhisConf_dhisOrgUnit(self):
        """Test DHIS_Conf table has valid dhisOrgUnit"""
        self.assertEqual(self.settingsDHIS[0].dhisOrgUnit, "SCVeBskgiK6")
    def test_6_dhisConf_dhisOrgUnit_Exception(self):
        """configDHIS should fail with invalid dhisOrgUnit."""
        c = self.copy_conn.cursor()
        sql = "UPDATE DHIS_Conf SET dhisOrgUnit = ?"
        par = ("",)
        c.execute(sql, par)
        self.assertRaises(transferDB.DHISConfigurationError,
                          self.copy_xferDB.configDHIS,
                          self.copy_conn, self.algorithm)
        self.copy_conn.rollback()

# Test VA Storage Configuration
class Check_7_DHIS_storeVA(unittest.TestCase):

    shutil.copy("OpenVAFiles/sample_newStorage.csv",
                "OpenVAFiles/newStorage.csv")

    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    pipelineRunDate = datetime.datetime.now()
    wrong_dbKey = "wrongKey"
    dbDirectory = "."

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey,
                        plRunDate = pipelineRunDate)
    conn = xferDB.connectDB()
    xferDB.configPipeline(conn)

    def test_DHIS_7_storeVA(self):
        """Check that VA records get stored in Transfer DB."""
        self.xferDB.storeVA(self.conn)
        c = self.conn.cursor()
        sql = "SELECT id FROM VA_Storage"
        c.execute(sql)
        vaIDs = c.fetchall()
        vaIDsList = [j for i in vaIDs for j in i]
        s1 = set(vaIDsList)
        dfNewStorage = read_csv("OpenVAFiles/newStorage.csv")
        dfNewStorageID = dfNewStorage['odkMetaInstanceID']
        s2 = set(dfNewStorageID)
        self.assertTrue(s2.issubset(s1))

class Check_8_updateODKLastRun(unittest.TestCase):
    """Test methods that updates ODK_Conf.odkLastRun"""
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    # dbDirectory = os.path.abspath(os.path.dirname(__file__))
    dbDirectory = "."
    pipelineRunDate = datetime.datetime.now()
    algorithm = "InterVA"

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey,
                             plRunDate = pipelineRunDate)
    copy_conn = copy_xferDB.connectDB()
    settingsODK = copy_xferDB.configODK(copy_conn)
    newRunDate = "3000-01-01_00:00:01"

    def test_8_method(self):
        """updateODKLastRun should change the date."""

        oldRunDate = self.settingsODK.odkLastRun

        self.copy_xferDB.updateODKLastRun(self.copy_conn, self.newRunDate)
        c = self.copy_conn.cursor()
        sql = "SELECT odkLastRun FROM ODK_Conf"
        c.execute(sql)
        sqlQuery = c.fetchall()
        for i in sqlQuery:
            updatedRunDate = i[0]
        self.assertEqual(updatedRunDate, self.newRunDate)
        self.copy_xferDB.updateODKLastRun(self.copy_conn, oldRunDate)

# Test for checkDuplicates
# Test EventLog Configuration

# Test Cleanup
    # def test_DHIS_cleanPipeline(self):

    #     shutil.copy("OpenVAFiles/sampleEAV.csv",
    #                 "OpenVAFiles/entityAttributeValue.csv")
    #     shutil.copy("OpenVAFiles/sample_openVA_input.csv",
    #                 "OpenVAFiles/openVA_input.csv")
    #     shutil.copy("ODKFiles/previous_bc_export.csv",
    #                 "ODKFiles/odkVCExportPrev.csv")
    #     shutil.copy("ODKFiles/another_bc_export.csv",
    #                 "ODKFiles/odkVCExportNew.csv")

    #     pipelineDHIS = dhis.DHIS(self.settingsDHIS, ".")
    #     apiDHIS = pipelineDHIS.connect()
    #     postLog = pipelineDHIS.postVA(apiDHIS)
    #     pipelineDHIS.verifyPost(postLog, apiDHIS)
    #     dfNewStorage = pd.read_csv("OpenVAFiles/newStorage.csv")
    #     nPushed = sum(dfNewStorage['pipelineOutcome'] == "Pushed to DHIS2")
    #     print(nPushed)
    #     self.assertEqual(nPushed, pipelineDHIS.nPostedRecords)



if __name__ == "__main__":
    unittest.main()
