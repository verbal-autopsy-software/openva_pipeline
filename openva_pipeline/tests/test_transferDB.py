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
from context import pipeline
from context import transferDB
from transferDB import TransferDB
from pysqlcipher3 import dbapi2 as sqlcipher


# Test valid DB connection & structure (i.e. all tables are there)
# (note: tests are executed in order of test names sorted as strings)
class Check_1_Connection(unittest.TestCase):

    # parameters for connecting to DB (assuming DB is in tests folder)
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    dbDirectory = os.path.abspath(os.path.dirname(__file__))

    def test_1_connection_1_dbFilePresent(self):
        """Check that DB file is located in path."""

        badPath = "/invalid/path/to/pipelineDB"
        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = badPath,
                            dbKey = self.dbKey)

        self.assertRaises(transferDB.DatabaseConnectionError, xferDB.connectDB)

    def test_1_connection_2_connection(self):
        """Test that the pipeline can connect to the DB."""

        xferDB = TransferDB(dbFileName = self.dbFileName,
                            dbDirectory = self.dbDirectory,
                            dbKey = self.dbKey)

        conn = xferDB.connectDB()
        c = conn.cursor()
        sqlTestConnection = "SELECT name FROM SQLITE_MASTER where type = 'table';"
        c.execute(sqlTestConnection)
        tableNames = c.fetchall()
        conn.close()
        self.assertTrue(len(tableNames) > 0)

# Check Pipeline Configuration
class Check_2_Pipeline_Conf(unittest.TestCase):
    """Test methods that grab configuration settings for pipline."""

    # parameters for connecting to DB (assuming DB is in tests folder)
    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    dbDirectory = os.path.abspath(os.path.dirname(__file__))

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey)
    conn = xferDB.connectDB()
    c = conn.cursor()
    c.execute("SELECT dhisCode from Algorithm_Metadata_Options;")
    metadataQuery = c.fetchall()
    settingsPipeline = xferDB.configPipeline(conn)

    copy_xferDB = TransferDB(dbFileName = "copy_Pipeline.db",
                             dbDirectory = dbDirectory,
                             dbKey = dbKey)
    copy_conn = copy_xferDB.connectDB()

    def test_2_pipelineConf_algorithmMetadataCode(self):
        """Test Pipeline_Conf table has valid algorithmMetadataCode."""
        validMetadataCode = self.settingsPipeline.algorithmMetadataCode in \
            [j for i in self.metadataQuery for j in i]
        self.assertTrue(validMetadataCode)

    def test_2_pipelineConf_algorithmMetadataCode_Exception(self):
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
            ("InSilico", "InSilico2016", "InterVA4", "InterVA5", "SmartVA")

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

# Check Configuration methods
class Check_3_ODK_Conf(unittest.TestCase):
    """Test methods that grab ODK configuration."""

    dbFileName = "Pipeline.db"
    dbKey = "enilepiP"
    dbDirectory = os.path.abspath(os.path.dirname(__file__))

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey)
    conn = xferDB.connectDB()

    settingsODK = xferDB.configODK(conn)

    def test_3_odkConf_odkURL(self):
        """Test ODK_Conf table has valid odkURL"""

        startHTML = self.settingsODK.odkURL[0:7]
        startHTMLS = self.settingsODK.odkURL[0:8]
        validURL = startHTML == "http://" or startHTMLS == "https://"
        self.assertTrue(validURL)

    def test_3_odkConf_odkUser(self):
        """Test ODK_Conf table has valid odkUser"""
        self.assertEqual(self.settingsODK.odkUser, "odk_openva")

    def test_3_odkConf_odkPassword(self):
        """Test ODK_Conf table has valid odkPassword"""
        self.assertEqual(self.settingsODK.odkPassword, "openVA2018")

    def test_3_odkConf_odkFormID(self):
        """Test ODK_Conf table has valid odkFormID"""
        self.assertEqual(self.settingsODK.odkFormID, "PHMRC_Shortened_Instrument_8_20_2015")

    def test_3_odkConf_odkLastRun(self):
        """Test ODK_Conf table has valid odkLastRun"""
        self.assertEqual(self.settingsODK.odkLastRun, "1900-01-01_00:00:01")

    def test_3_odkConf_odkLastRunDate(self):
        """Test ODK_Conf table has valid odkLastRunDate"""
        self.assertEqual(self.settingsODK.odkLastRunDate, "1900/01/01")

    def test_3_odkConf_odkLastRunDatePrev(self):
        """Test ODK_Conf table has valid odkLastRunDatePrev"""
        self.assertEqual(self.settingsODK.odkLastRunDatePrev, "1899/12/31")

    def test_3_odkConf_odkLastRunResult(self):
        """Test ODK_Conf table has valid odkLastRunResult"""
        self.assertIn(self.settingsODK.odkLastRunResult, (0,1))

# Test invalid calls to DB

# Test when Transfer DB has invalid info / setup

if __name__ == "__main__":
    unittest.main()
