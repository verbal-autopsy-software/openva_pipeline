from openva_pipeline.transfer_db import TransferDB
from openva_pipeline.pipeline import Pipeline
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
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        now_date = datetime.datetime.now()
        pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="Pipeline.db", db_directory=".",
                             db_key="enilepiP", pl_run_date=pipeline_run_date)
        conn = xfer_db._connect_db()
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
        self.assertIn("Pipeline_Conf", self.list_table_names)

    def test_table_va_storage(self):
        """Test that the Pipeline.db has VA_Storage table."""
        self.assertIn("VA_Storage", self.list_table_names)

    def test_table_eventLog(self):
        """Test that the Pipeline.db has EventLog table."""
        self.assertIn("EventLog", self.list_table_names)

    def test_table_odk_conf(self):
        """Test that the Pipeline.db has ODK_Conf table."""
        self.assertIn("ODK_Conf", self.list_table_names)

    def test_table_interva_conf(self):
        """Test that the Pipeline.db has InterVA_Conf table."""
        self.assertIn("InterVA_Conf", self.list_table_names)

    def test_table_advanced_interva_conf(self):
        """Test that the Pipeline.db has Advanced_InterVA_Conf table."""
        self.assertIn("Advanced_InterVA_Conf", self.list_table_names)

    def test_table_insilicova_conf(self):
        """Test that the Pipeline.db has InSilicoVA_Conf table."""
        self.assertIn("InSilicoVA_Conf", self.list_table_names)

    def test_table_advanced_insilicova_conf(self):
        """Test that the Pipeline.db has Advanced_InSilicoVA_Conf table."""
        self.assertIn("Advanced_InSilicoVA_Conf", self.list_table_names)

    def test_table_smartva_conf(self):
        """Test that the Pipeline.db has SmartVA_Conf table."""
        self.assertIn("SmartVA_Conf", self.list_table_names)

    def test_table_smartva_country(self):
        """Test that the Pipeline.db has SmartVA_Country table."""
        self.assertIn("SmartVA_Country", self.list_table_names)

    def test_table_dhis_conf(self):
        """Test that the Pipeline.db has DHIS_Conf table."""
        self.assertIn("DHIS_Conf", self.list_table_names)

    def test_table_cod_codes_dhis(self):
        """Test that the Pipeline.db has COD_Codes_DHIS table."""
        self.assertIn("COD_Codes_DHIS", self.list_table_names)

    def test_table_algorithm_metadata_options(self):
        """Test that the Pipeline.db has Algorithm_Metadata_Options table."""
        self.assertIn("Algorithm_Metadata_Options", self.list_table_names)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckDBConnectionExceptions(unittest.TestCase):

    def test_db_file_present_exception(self):
        """Check that DB file is located in path."""
        bad_path = "/invalid/path/to/pipelineDB"
        now_date = datetime.datetime.now()
        pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="Pipeline.db", db_directory=bad_path,
                             db_key="enilepiP", pl_run_date=pipeline_run_date)
        self.assertRaises(DatabaseConnectionError, xfer_db._connect_db)

    def test_wrong_key_exception(self):
        """Pipeline should raise an error when the wrong key is used."""
        pipeline_run_date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="Pipeline.db", db_directory=".",
                             db_key="wrong_db_key",
                             pl_run_date=pipeline_run_date)
        self.assertRaises(DatabaseConnectionError, xfer_db._connect_db)


class CheckPipelineConf(unittest.TestCase):
    """Test methods that grab configuration settings for pipeline."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        now_date = datetime.datetime.now()
        pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")

        cls.xfer_db = TransferDB(db_file_name=db_file_name,
                                 db_directory=db_directory,
                                 db_key=db_key,
                                 pl_run_date=pipeline_run_date)
        cls.settings_pipeline = cls.xfer_db.config_pipeline()

        cls.copy_xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)
        # parameters for connecting to DB with wrong Tables
        wrong_tables_db_file_name = "wrongTables_Pipeline.db"
        cls.wrong_tables_xfer_db = TransferDB(
            db_file_name=wrong_tables_db_file_name,
            db_directory=db_directory,
            db_key=db_key,
            pl_run_date=pipeline_run_date)
        # parameters for connecting to DB with wrong fields
        wrong_fields_db_file_name = "wrongFields_Pipeline.db"
        cls.wrong_fields_xfer_db = TransferDB(
            db_file_name=wrong_fields_db_file_name,
            db_directory=db_directory,
            db_key=db_key,
            pl_run_date=pipeline_run_date)

    def test_pipeline_conf_exception_no_table(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(PipelineConfigurationError,
                          self.wrong_tables_xfer_db.config_pipeline)

    def test_pipeline_conf_exception_no_field(self):
        """Test that Pipeline raises error if no Pipeline_Conf table."""
        self.assertRaises(PipelineConfigurationError,
                          self.wrong_fields_xfer_db.config_pipeline)

    # Thinking about removing this check -- 2021-06-09 (jt)
    # def test_pipeline_conf_algorithmMetadataCode(self):
    #     """Test Pipeline_Conf table has valid algorithmMetadataCode."""

    #     validMetadataCode = self.settings_pipeline.algorithmMetadataCode in \
    #         [j for i in self.metadataQuery for j in i]
    #     self.assertTrue(validMetadataCode)

    # def test_pipeline_conf_algorithmMetadataCode_Exception_value(self):
    #     """config_pipeline should fail with invalid algorithmMetadataCode."""

    #     c = self.copy_conn.cursor()
    #     sql = "UPDATE Pipeline_Conf SET algorithmMetadataCode = ?"
    #     par = ("wrong",)
    #     c.execute(sql, par)
    #     self.assertRaises(PipelineConfigurationError,
    #                       self.copy_xfer_db.config_pipeline, self.copy_conn)
    #     self.copy_conn.rollback()

    def test_pipeline_conf_cod_source(self):
        """Test Pipeline_Conf table has valid cod_source"""
        valid_cod_source = self.settings_pipeline.cod_source in \
            ("ICD10", "WHO", "Tariff")
        self.assertTrue(valid_cod_source)

    def test_pipeline_conf_cod_source_exception(self):
        """config_pipeline should fail with invalid codSource."""
        self.copy_xfer_db.update_table("Pipeline_Conf",
                                       "codSource",
                                       "wrong")
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xfer_db.config_pipeline)
        self.copy_xfer_db.update_table("Pipeline_Conf",
                                       "codSource",
                                       self.settings_pipeline.cod_source)

    def test_pipeline_conf_algorithm(self):
        """Test Pipeline_Conf table has valid algorithm"""
        valid_algorithm = self.settings_pipeline.algorithm in \
            ("InSilicoVA", "InterVA", "SmartVA")
        self.assertTrue(valid_algorithm)

    def test_pipeline_conf_algorithm_exception(self):
        """config_pipeline should fail with invalid algorithm."""
        self.copy_xfer_db.update_table("Pipeline_Conf",
                                       "algorithm",
                                       "wrong")
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xfer_db.config_pipeline)
        self.copy_xfer_db.update_table("Pipeline_Conf",
                                       "algorithm",
                                       self.settings_pipeline.algorithm)

    def test_pipeline_conf_working_directory(self):
        """Test Pipeline_Conf table has valid algorithm"""
        valid_wd = os.path.isdir(self.settings_pipeline.working_directory)
        self.assertTrue(valid_wd)

    def test_pipeline_conf_working_directory_exception(self):
        """config_pipeline should fail with invalid working_directory."""
        self.copy_xfer_db.update_table("Pipeline_Conf",
                                       "workingDirectory",
                                       "wrong")
        self.assertRaises(PipelineConfigurationError,
                          self.copy_xfer_db.config_pipeline)
        self.copy_xfer_db.update_table(
            "Pipeline_Conf",
            "workingDirectory",
            self.settings_pipeline.working_directory)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckODKConf(unittest.TestCase):
    """Test methods that grab ODK configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        pipeline_run_date = datetime.datetime.now().strftime(
            "%Y-%m-%d_%H:%M:%S")

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        cls.settings_odk = xfer_db.config_odk()

        cls.copy_xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)

    def test_odk_conf_odk_url(self):
        """Test ODK_Conf table has valid odkURL"""
        self.assertEqual(self.settings_odk.odk_url,
                         "https://odk-central.swisstph.ch")

    def test_odk_conf_odk_url_exception(self):
        """config_odk should fail with invalid url."""
        self.copy_xfer_db.update_table("ODK_Conf", "odkURL", "wrong")
        self.assertRaises(ODKConfigurationError,
                          self.copy_xfer_db.config_odk)
        self.copy_xfer_db.update_table("ODK_Conf",
                                       "odkURL",
                                       self.settings_odk.odk_url)

    def test_odk_conf_odk_user(self):
        """Test ODK_Conf table has valid odkUser"""
        self.assertEqual(self.settings_odk.odk_user, "who.va.view@swisstph.ch")

    def test_odk_conf_odk_password(self):
        """Test ODK_Conf table has valid odkPassword"""
        self.assertEqual(self.settings_odk.odk_password, "WHOVAVi3w153!")

    def test_odk_conf_odk_form_id(self):
        """Test ODK_Conf table has valid odkFormID"""
        self.assertEqual(self.settings_odk.odk_form_id, "va_who_v1_5_3")

    def test_odk_conf_odk_last_run(self):
        """Test ODK_Conf table has valid odkLastRun"""
        self.assertEqual(self.settings_odk.odk_last_run, "1900-01-01_00:00:01")

    def test_odk_conf_odk_last_run_date(self):
        """Test ODK_Conf table has valid odkLastRunDate"""
        self.assertEqual(self.settings_odk.odk_last_run_date, "1900/01/01")

    def test_odk_conf_odk_last_run_date_prev(self):
        """Test ODK_Conf table has valid odk_last_run_date_prev"""
        self.assertEqual(self.settings_odk.odk_last_run_date_prev,
                         "1899/12/31")

    def test_odk_conf_odk_use_central(self):
        """Test ODK_Conf table has valid odkUseCentral"""
        self.assertEqual(self.settings_odk.odk_use_central, "True")

    def test_odk_conf_odk_project_number(self):
        """Test ODK_Conf table has valid odkProjectNumber"""
        self.assertEqual(self.settings_odk.odk_project_number, "40")

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckOpenVAConfInterVA(unittest.TestCase):
    """Test methods that grab InterVA configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        pipeline_run_date = datetime.datetime.now().strftime(
            "%Y-%m-%d_%H:%M:%S")

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        cls.settings_pipeline = xfer_db.config_pipeline()
        cls.settings_openva = xfer_db.config_openva("InterVA")

        cls.copy_xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)

    def test_openva_conf_interva_version(self):
        """Test InterVA_Conf table has valid version"""
        self.assertIn(self.settings_openva.interva_version, ("4", "5"))

    def test_openva_conf_interva_version_exception(self):
        """config_openva should fail with invalid 
        InterVA_Conf.Version value."""
        self.copy_xfer_db.update_table("InterVA_Conf",
                                       "Version",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table(
            "InterVA_Conf",
            "Version",
            self.settings_openva.interva_version)

    def test_openva_conf_interva_hiv(self):
        """Test InterVA_Conf table has valid HIV"""
        self.assertIn(self.settings_openva.interva_hiv, ("v", "l", "h"))

    def test_openva_conf_interva_hiv_exception(self):
        """config_openva should fail with invalid InterVA_Conf.HIV value."""
        self.copy_xfer_db.update_table("InterVA_Conf",
                                       "HIV",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table("InterVA_Conf",
                                       "HIV",
                                       self.settings_openva.interva_hiv)

    def test_openva_conf_interva_malaria(self):
        """Test InterVA_Conf table has valid Malaria"""
        self.assertIn(self.settings_openva.interva_malaria, ("v", "l", "h"))

    def test_openva_conf_interva_malaria_exception(self):
        """config_openva should fail with invalid 
        InterVA_Conf.Malaria value."""
        self.copy_xfer_db.update_table("InterVA_Conf",
                                       "Malaria",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table("InterVA_Conf",
                                       "Malaria",
                                       self.settings_openva.interva_malaria)

    def test_openva_conf_interva_output(self):
        """Test Advanced_InterVA_Conf table has valid output."""
        self.assertIn(self.settings_openva.interva_output,
                      ("classic", "extended"))

    def test_openva_conf_interva_output_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.output value."""
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "output",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "output",
                                       self.settings_openva.interva_output)

    def test_openva_conf_interva_append(self):
        """Test Advanced_InterVA_Conf table has valid append."""
        self.assertIn(self.settings_openva.interva_append,
                      ("TRUE", "FALSE"))

    def test_openva_conf_interva_append_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.append value."""
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "append",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "append",
                                       self.settings_openva.interva_append)

    def test_openva_conf_interva_groupcode(self):
        """Test Advanced_InterVA_Conf table has valid groupcode."""
        self.assertIn(self.settings_openva.interva_groupcode,
                      ("TRUE", "FALSE"))

    def test_openva_conf_interva_groupcode_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.groupcode value."""
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "groupcode",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "groupcode",
                                       self.settings_openva.interva_groupcode)

    def test_openva_conf_interva_replicate(self):
        """Test Advanced_InterVA_Conf table has valid replicate."""
        self.assertIn(self.settings_openva.interva_replicate,
                      ("TRUE", "FALSE"))

    def test_openva_conf_interva_replicate_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.replicate value."""
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "replicate",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "replicate",
                                       self.settings_openva.interva_replicate)

    def test_openva_conf_interva_replicate_bug1(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug1."""
        self.assertIn(self.settings_openva.interva_replicate_bug1,
                      ("TRUE", "FALSE"))

    def test_openva_conf_interva_replicate_bug1_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.replicate_bug1 value."""
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "replicate_bug1",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table(
            "Advanced_InterVA_Conf",
            "replicate_bug1",
            self.settings_openva.interva_replicate_bug1)

    def test_openva_conf_interva_replicate_bug2(self):
        """Test Advanced_InterVA_Conf table has valid replicate_bug2."""
        self.assertIn(self.settings_openva.interva_replicate_bug2,
                      ("TRUE", "FALSE"))

    def test_openva_conf_interva_replicate_bug2_exception(self):
        """config_openva should fail with invalid 
        Advanced_InterVA_Conf.replicate_bug2 value."""
        self.copy_xfer_db.update_table("Advanced_InterVA_Conf",
                                       "replicate_bug2",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InterVA")
        self.copy_xfer_db.update_table(
            "Advanced_InterVA_Conf",
            "replicate_bug2",
            self.settings_openva.interva_replicate_bug2)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckOpenVAConfInSilicoVA(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        pipeline_run_date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        cls.settings_pipeline = xfer_db.config_pipeline()
        cls.settings_openva = xfer_db.config_openva("InSilicoVA")

        cls.copy_xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)

    def test_openva_conf_insilicova_data_type(self):
        """Test InSilicoVA_Conf table has valid data_type"""
        self.assertIn(self.settings_openva.insilicova_data_type,
                      ("WHO2012", "WHO2016"))

    def test_openva_conf_insilicova_data_type_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.data_type value."""
        self.copy_xfer_db.update_table("InSilicoVA_Conf",
                                       "data_type",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "InSilicoVA_Conf",
            "data_type",
            self.settings_openva.insilicova_data_type)

    def test_openva_conf_insilicova_nsim(self):
        """Test InSilicoVA_Conf table has valid Nsim"""
        self.assertEqual(self.settings_openva.insilicova_nsim, "4000")

    def test_openva_conf_insilicova_nsim_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.Nsim value."""
        self.copy_xfer_db.update_table("InSilicoVA_Conf",
                                       "Nsim",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table("InSilicoVA_Conf",
                                       "Nsim",
                                       self.settings_openva.insilicova_nsim)

    def test_openva_conf_insilicova_is_numeric(self):
        """Test InSilicoVA_Conf table has valid isNumeric"""
        self.assertIn(self.settings_openva.insilicova_is_numeric,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_is_numeric_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.isNumeric value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "isNumeric",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "isNumeric",
            self.settings_openva.insilicova_is_numeric)

    def test_openva_conf_insilicova_update_cond_prob(self):
        """Test InSilicoVA_Conf table has valid updateCondProb"""
        self.assertIn(self.settings_openva.insilicova_update_cond_prob,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_update_cond_prob_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.updateCondProb value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "updateCondProb",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "updateCondProb",
            self.settings_openva.insilicova_update_cond_prob)

    def test_openva_conf_insilicova_keep_probbase_level(self):
        """Test InSilicoVA_Conf table has valid keepProbbase_level"""
        self.assertIn(self.settings_openva.insilicova_keep_probbase_level,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_keep_probbase_level_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.keepProbbase_level value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "keepProbbase_level",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "keepProbbase_level",
            self.settings_openva.insilicova_keep_probbase_level)

    def test_openva_conf_insilicova_cond_prob(self):
        """Test InSilicoVA_Conf table has valid CondProb"""
        self.assertEqual(self.settings_openva.insilicova_cond_prob, "NULL")

    def test_openva_conf_insilicova_cond_prob_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.CondProb value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "CondProb",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "CondProb",
            self.settings_openva.insilicova_cond_prob)

    def test_openva_conf_insilicova_cond_prob_num(self):
        """Test InSilicoVA_Conf table has valid CondProbNum"""
        self.assertEqual(self.settings_openva.insilicova_cond_prob_num, "NULL")

    def test_openva_conf_insilicova_cond_prob_num_2(self):
        """config_openva should accept InSilicoVA_Conf.CondProbNum ".35"."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "CondProbNum",
                                       ".35")
        new_test = self.copy_xfer_db.config_openva("InSilicoVA")
        self.assertEqual(new_test.insilicova_cond_prob_num, ".35")

    def test_openva_conf_insilicova_cond_prob_num_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.CondProbNum value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "CondProbNum",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "CondProbNum",
            self.settings_openva.insilicova_cond_prob_num)

    def test_openva_conf_insilicova_datacheck(self):
        """Test InSilicoVA_Conf table has valid datacheck"""
        self.assertIn(self.settings_openva.insilicova_datacheck,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_datacheck_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.datacheck value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "datacheck",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "datacheck",
            self.settings_openva.insilicova_datacheck)

    def test_openva_conf_insilicova_datacheck_missing(self):
        """Test InSilicoVA_Conf table has valid datacheck_missing"""
        self.assertIn(self.settings_openva.insilicova_datacheck_missing,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_datacheck_missing_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.datacheck_missing value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "datacheck_missing",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "datacheck_missing",
            self.settings_openva.insilicova_datacheck_missing)

    def test_openva_conf_insilicova_external_sep(self):
        """Test InSilicoVA_Conf table has valid external_sep"""
        self.assertIn(self.settings_openva.insilicova_external_sep,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_external_sep_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.external_sep value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "external_sep",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "external_sep",
            self.settings_openva.insilicova_external_sep)

    def test_openva_conf_insilicova_thin(self):
        self.assertEqual(self.settings_openva.insilicova_thin, "10")

    def test_openva_conf_insilicova_thin_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.thin value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "thin",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "thin",
                                       self.settings_openva.insilicova_thin)

    def test_openva_conf_insilicova_burnin(self):
        """Test InSilicoVA_Conf table has valid burnin"""
        self.assertEqual(self.settings_openva.insilicova_burnin, "2000")

    def test_openva_conf_insilicova_burnin_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.burnin value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "burnin",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "burnin",
                                       self.settings_openva.insilicova_burnin)

    def test_openva_conf_insilicova_auto_length(self):
        """Test InSilicoVA_Conf table has valid auto_length"""
        self.assertIn(self.settings_openva.insilicova_auto_length,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_auto_length_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.auto_length value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "auto_length",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "auto_length",
            self.settings_openva.insilicova_auto_length)

    def test_openva_conf_insilicova_conv_csmf(self):
        """Test InSilicoVA_Conf table has valid conv_csmf"""
        self.assertEqual(self.settings_openva.insilicova_conv_csmf, "0.02")

    def test_openva_conf_insilicova_conv_csmf_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.conv_csmf value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "conv_csmf",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "conv_csmf",
            self.settings_openva.insilicova_conv_csmf)

    def test_openva_conf_insilicova_jump_scale(self):
        """Test InSilicoVA_Conf table has valid jump_scale"""
        self.assertEqual(self.settings_openva.insilicova_jump_scale, "0.1")

    def test_openva_conf_insilicova_jump_scale_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.jump_scale value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "jump_scale",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "jump_scale",
            self.settings_openva.insilicova_jump_scale)

    def test_openva_conf_insilicova_levels_prior(self):
        self.assertEqual(self.settings_openva.insilicova_levels_prior, "NULL")
        
    def test_openva_conf_insilicova_levels_prior_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.levels_prior value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "levels_prior",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "levels_prior",
            self.settings_openva.insilicova_levels_prior)

    def test_openva_conf_insilicova_levels_strength(self):
        self.assertEqual(self.settings_openva.insilicova_levels_strength, "1")

    def test_openva_conf_insilicova_levels_strength_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.levels_strength value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "levels_strength",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "levels_strength",
            self.settings_openva.insilicova_levels_strength)

    def test_openva_conf_insilicova_trunc_min(self):
        self.assertEqual(self.settings_openva.insilicova_trunc_min, "0.0001")

    def test_openva_conf_insilicova_trunc_min_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.trunc_min value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "trunc_min",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "trunc_min",
            self.settings_openva.insilicova_trunc_min)

    def test_openva_conf_insilicova_trunc_max(self):
        self.assertEqual(self.settings_openva.insilicova_trunc_max, "0.9999")

    def test_openva_conf_insilicova_trunc_max_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.trunc_max value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "trunc_max",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "trunc_max",
            self.settings_openva.insilicova_trunc_max)

    def test_openva_conf_insilicova_subpop(self):
        self.assertEqual(self.settings_openva.insilicova_subpop, "NULL")

    def test_openva_conf_insilicova_subpop_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.subpop value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "subpop",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "subpop",
                                       self.settings_openva.insilicova_subpop)

    def test_openva_conf_insilicova_java_option(self):
        self.assertEqual(self.settings_openva.insilicova_java_option, "-Xmx1g")

    def test_openva_conf_insilicova_java_option_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.java_option value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "java_option",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "java_option",
            self.settings_openva.insilicova_java_option)

    def test_openva_conf_insilicova_seed(self):
        self.assertEqual(self.settings_openva.insilicova_seed, "1")

    def test_openva_conf_insilicova_seed_Exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.seed value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "seed",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "seed",
                                       self.settings_openva.insilicova_seed)

    def test_openva_conf_insilicova_phy_code(self):
        self.assertEqual(self.settings_openva.insilicova_phy_code, "NULL")

    def test_openva_conf_insilicova_phy_code_Exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_code value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "phy_code",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "phy_code",
            self.settings_openva.insilicova_phy_code)

    def test_openva_conf_insilicova_phy_cat(self):
        self.assertEqual(self.settings_openva.insilicova_phy_cat, "NULL")

    def test_openva_conf_insilicova_phy_cat_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_cat value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "phy_cat",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "phy_cat",
            self.settings_openva.insilicova_phy_cat)

    def test_openva_conf_insilicova_phy_unknown(self):
        self.assertEqual(self.settings_openva.insilicova_phy_unknown, "NULL")

    def test_openva_conf_insilicova_phy_unknown_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_unknown value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "phy_unknown",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "phy_unknown",
            self.settings_openva.insilicova_phy_unknown)

    def test_openva_conf_insilicova_phy_external(self):
        self.assertEqual(self.settings_openva.insilicova_phy_external, "NULL")

    def test_openva_conf_insilicova_phy_external_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_external value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "phy_external",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "phy_external",
            self.settings_openva.insilicova_phy_external)

    def test_openva_conf_insilicova_phy_debias(self):
        self.assertEqual(self.settings_openva.insilicova_phy_debias, "NULL")

    def test_openva_conf_insilicova_phy_debias_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.phy_debias value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "phy_debias",
                                       "")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "phy_debias",
            self.settings_openva.insilicova_phy_debias)

    def test_openva_conf_insilicova_exclude_impossible_cause(self):
        self.assertIn(self.settings_openva.insilicova_exclude_impossible_cause,
                      ("subset", "all", "InterVA", "none"))

    def test_openva_conf_insilicova_exclude_impossible_cause_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.exclude_impossible_cause value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "exclude_impossible_cause",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "exclude_impossible_cause",
            self.settings_openva.insilicova_exclude_impossible_cause)

    def test_openva_conf_insilicova_no_is_missing(self):
        self.assertIn(self.settings_openva.insilicova_no_is_missing,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_no_is_missing_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.no_is_missing value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "no_is_missing",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "no_is_missing",
            self.settings_openva.insilicova_no_is_missing)

    def test_openva_conf_insilicova_indiv_ci(self):
        self.assertEqual(self.settings_openva.insilicova_indiv_ci, "NULL")

    def test_openva_conf_insilicova_indiv_ci_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.indiv_CI value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "indiv_CI",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "indiv_CI",
            self.settings_openva.insilicova_indiv_ci)

    def test_openva_conf_insilicova_groupcode(self):
        self.assertIn(self.settings_openva.insilicova_groupcode,
                      ("TRUE", "FALSE"))

    def test_openva_conf_insilicova_groupcode_exception(self):
        """config_openva should fail with invalid 
        InSilicoVA_Conf.groupcode value."""
        self.copy_xfer_db.update_table("Advanced_InSilicoVA_Conf",
                                       "groupcode",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "InSilicoVA")
        self.copy_xfer_db.update_table(
            "Advanced_InSilicoVA_Conf",
            "groupcode",
            self.settings_openva.insilicova_groupcode)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckSmartVAConf(unittest.TestCase):
    """Test methods that grab InSilicoVA configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        pipeline_run_date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        cls.settings_smartva = xfer_db.config_openva("SmartVA")

        cls.copy_xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)

    def test_smartva_conf_country(self):
        """Test SmartVA_Conf table has valid country"""
        self.assertEqual(self.settings_smartva.smartva_country, "Unknown")

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.country value."""
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "country",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "SmartVA")
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "country",
                                       self.settings_smartva.smartva_country)

    def test_smartva_conf_hiv(self):
        """Test SmartVA_Conf table has valid hiv"""
        self.assertEqual(self.settings_smartva.smartva_hiv, "False")

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.hiv value."""
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "hiv",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "SmartVA")
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "hiv",
                                       self.settings_smartva.smartva_hiv)

    def test_smartva_conf_malaria(self):
        """Test SmartVA_Conf table has valid malaria"""
        self.assertEqual(self.settings_smartva.smartva_malaria, "False")

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.malaria value."""
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "malaria",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "SmartVA")
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "malaria",
                                       self.settings_smartva.smartva_malaria)

    def test_smartva_conf_hce(self):
        """Test SmartVA_Conf table has valid hce"""
        self.assertEqual(self.settings_smartva.smartva_hce, "False")

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid SmartVA_Conf.hce value."""
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "hce",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "SmartVA")
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "hce",
                                       self.settings_smartva.smartva_hce)

    def test_smartva_conf_freetext(self):
        """Test SmartVA_Conf table has valid freetext"""
        self.assertEqual(self.settings_smartva.smartva_freetext, "False")

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.freetext value."""
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "freetext",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "SmartVA")
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "freetext",
                                       self.settings_smartva.smartva_freetext)

    def test_smartva_conf_figures(self):
        """Test SmartVA_Conf table has valid figures"""
        self.assertEqual(self.settings_smartva.smartva_figures, "False")

    def test_smartva_conf_data_type_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.figures value."""
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "figures",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "SmartVA")
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "figures",
                                       self.settings_smartva.smartva_figures)

    def test_smartva_conf_language(self):
        """Test SmartVA_Conf table has valid language"""
        self.assertEqual(self.settings_smartva.smartva_language, "english")

    def test_smartva_conf_language_exception(self):
        """config_openva should fail with invalid 
        SmartVA_Conf.language value."""
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "language",
                                       "wrong")
        self.assertRaises(OpenVAConfigurationError,
                          self.copy_xfer_db.config_openva,
                          "SmartVA")
        self.copy_xfer_db.update_table("SmartVA_Conf",
                                       "language",
                                       self.settings_smartva.smartva_language)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckDHISConf(unittest.TestCase):
    """Test methods that grab DHIS configuration."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        pipeline_run_date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        cls.algorithm = "InterVA"

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        cls.settings_dhis = xfer_db.config_dhis(cls.algorithm)

        cls.copy_xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)

    def test_dhis_conf_dhis_url(self):
        """Test DHIS_Conf table has valid dhisURL"""
        self.assertEqual(self.settings_dhis[0].dhis_url,
                         "https://va30tr.swisstph-mis.ch")

    def test_dhis_conf_dhis_url_exception(self):
        """config_dhis should fail with invalid url."""
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisURL",
                                       "wrong")
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.algorithm)
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisURL",
                                       self.settings_dhis[0].dhis_url)

    def test_dhis_conf_dhis_user(self):
        """Test DHIS_Conf table has valid dhisUser"""
        self.assertEqual(self.settings_dhis[0].dhis_user, "va-demo")

    def test_dhis_conf_dhis_user_exception(self):
        """config_dhis should fail with invalid dhisUser."""
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisUser",
                                       "")
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.algorithm)
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisUser",
                                       self.settings_dhis[0].dhis_user)

    def test_dhis_conf_dhis_password(self):
        """Test DHIS_Conf table has valid dhisPassword"""
        self.assertEqual(self.settings_dhis[0].dhis_password,
                         "VerbalAutopsy99!")

    def test_dhis_conf_dhis_password_exception(self):
        """config_dhis should fail with invalid dhisPassword."""
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisPassword",
                                       "")
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.algorithm)
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisPassword",
                                       self.settings_dhis[0].dhis_password)

    def test_dhis_conf_dhis_org_unit(self):
        """Test DHIS_Conf table has valid dhisOrgUnit"""
        self.assertEqual(self.settings_dhis[0].dhis_org_unit, "SCVeBskgiK6")

    def test_dhis_conf_dhis_post_root(self):
        """Test DHIS_Conf table has valid dhisPostRoot"""
        self.assertEqual(self.settings_dhis[0].dhis_post_root, "False")

    def test_dhis_conf_dhis_org_unit_exception(self):
        """config_dhis should fail with invalid dhisOrgUnit."""
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisOrgUnit",
                                       "")
        self.assertRaises(DHISConfigurationError,
                          self.copy_xfer_db.config_dhis,
                          self.algorithm)
        self.copy_xfer_db.update_table("DHIS_Conf",
                                       "dhisOrgUnit",
                                       self.settings_dhis[0].dhis_org_unit)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckDHISStoreVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        shutil.copy("OpenVAFiles/sample_new_storage.csv",
                    "OpenVAFiles/new_storage.csv")
        shutil.copy("OpenVAFiles/sample_record_storage.csv",
                    "OpenVAFiles/record_storage.csv")
        shutil.copy("OpenVAFiles/sample_eav.csv",
                    "OpenVAFiles/entity_attribute_value.csv")

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        pipeline_run_date = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        db_directory = "."

        cls.xfer_db = TransferDB(db_file_name=db_file_name,
                                 db_directory=db_directory,
                                 db_key=db_key,
                                 pl_run_date=pipeline_run_date)
        cls.xfer_db.config_pipeline()
        cls.conn = cls.xfer_db._connect_db()
        pl = Pipeline(db_file_name,
                      db_directory,
                      db_key,
                      True)
        pl.run_dhis()

    def test_dhis_store_va(self):
        """Check that VA records get stored in Transfer DB."""

        self.xfer_db.store_va()
        c = self.conn.cursor()
        sql = "SELECT id FROM VA_Storage"
        c.execute(sql)
        va_ids = c.fetchall()
        va_ids_list = [j for i in va_ids for j in i]
        s1 = set(va_ids_list)
        df_new_storage = read_csv("OpenVAFiles/new_storage.csv")
        df_new_storage_id = df_new_storage["odkMetaInstanceID"]
        s2 = set(df_new_storage_id)
        self.assertTrue(s2.issubset(s1))

    def test_store_no_ou_va(self):
        """Check that VA records without a valid org unit get stored."""

        df_record_storage = read_csv("OpenVAFiles/record_storage.csv")
        va_record = df_record_storage.iloc[0].to_dict()
        df_eav = read_csv("OpenVAFiles/entity_attribute_value.csv")
        grouped = df_eav.groupby(["ID"])
        eav = grouped.get_group(va_record["id"])
        self.xfer_db.store_no_ou_va(va_record,
                                    eav,
                                    "Region, State, Village")
        c = self.conn.cursor()
        sql = "SELECT id, eventBlob, evaBlob FROM VA_Org_Unit_Not_Found"
        c.execute(sql)
        sql_results = c.fetchall()
        va_ids_list = [i[0] for i in sql_results]
        # va_from_storage = [i[1] for i in sql_results]
        # json.loads(va_record[0])
        va_eva = [i[2] for i in sql_results]
        self.assertTrue(va_record["id"] in va_ids_list)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckUpdateODKLastRun(unittest.TestCase):
    """Test methods that updates ODK_Conf.odk_last_run"""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        db_key = "enilepiP"
        db_directory = "."
        pipeline_run_date = datetime.datetime.now().strftime(
            "%Y-%m-%d_%H:%M:%S")

        cls.copy_xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                                      db_directory=db_directory,
                                      db_key=db_key,
                                      pl_run_date=pipeline_run_date)
        cls.copy_conn = cls.copy_xfer_db._connect_db()
        cls.settings_odk = cls.copy_xfer_db.config_odk()

    def test_method(self):
        """update_odk_last_run should change the date."""

        self.copy_xfer_db.update_odk_last_run()
        c = self.copy_conn.cursor()
        sql = "SELECT odkLastRun FROM ODK_Conf"
        c.execute(sql)
        sql_query = c.fetchall()
        for i in sql_query:
            updated_run_date = i[0]
        self.assertEqual(updated_run_date, self.copy_xfer_db.pl_run_date)

    @classmethod
    def tearDownClass(cls):
        os.remove("Pipeline.db")


class CheckUpdateTableConf(unittest.TestCase):
    """Test method for updating configuration tables (and the getter)."""

    @classmethod
    def setUpClass(cls):
        if not os.path.isfile("test_conf_pipeline.db"):
            create_transfer_db("test_conf_pipeline.db", ".", "enilepiP")
        db_file_name = "test_conf_pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        pipeline_run_date = datetime.datetime.now().strftime(
            "%Y-%m-%d_%H:%M:%S")

        cls.test_db = TransferDB(db_file_name=db_file_name,
                                 db_directory=db_directory,
                                 db_key=db_key,
                                 pl_run_date=pipeline_run_date)

    def test_update_odk_conf(self):
        """should change values in ODK_Conf table"""
        list_fields = ["odkID", "odkURL", "odkUser", "odkPassword",
                       "odkFormID", "odkLastRun", "odkUseCentral",
                       "odkProjectNumber"]
        list_values = ["new_id", "new_url", "new_user", "new_password",
                       "new_form_id", "new_last_run", "True",
                       "new_project_number"]
        self.test_db.update_table("ODK_Conf",
                                  list_fields,
                                  list_values)
        results = self.test_db.get_table_conf("ODK_Conf")
        for i in list_values:
            with self.subTest(i=i):
                self.assertTrue(i in results[0])

    def test_update_dhis_conf(self):
        """should change values in DHIS_Conf table"""
        list_fields = ["dhisURL", "dhisUser", "dhisPassword",
                       "dhisOrgUnit", "dhisPostRoot"]
        list_values = ["new_url", "new_user", "new_password",
                       "new_org_unit", "True"]
        self.test_db.update_table("DHIS_Conf",
                                  list_fields,
                                  list_values)
        results = self.test_db.get_table_conf("DHIS_Conf")
        for i in list_values:
            with self.subTest(i=i):
                self.assertTrue(i in results[0])

    def test_update_pipeline_conf(self):
        """should change values in Pipeline_Conf table"""
        list_fields = ["algorithmMetadataCode", "codSource", "algorithm",
                       "workingDirectory"]
        list_values = ["new_amc", "ICD10", "InSilicoVA",
                       "new_working_directory"]
        self.test_db.update_table("Pipeline_Conf",
                                  list_fields,
                                  list_values)
        results = self.test_db.get_table_conf("Pipeline_Conf")
        for i in list_values:
            with self.subTest(i=i):
                self.assertTrue(i in results[0])

    def test_get_tables(self):
        """should get all of the table names in transfer db"""
        table_names = ["Pipeline_Conf", "VA_Storage", "EventLog", "ODK_Conf",
                       "InterVA_Conf", "Advanced_InterVA_Conf",
                       "InSilicoVA_Conf", "Advanced_InSilicoVA_Conf",
                       "SmartVA_Conf", "SmartVA_Country", "DHIS_Conf",
                       "COD_Codes_DHIS", "Algorithm_Metadata_Options"]
        results = self.test_db.get_tables()
        for i in table_names:
            with self.subTest(i=i):
                self.assertTrue(i in results)

    def test_get_fields(self):
        """should get all of the fields in a table"""
        odk_fields = ["odkID", "odkURL", "odkUser", "odkPassword",
                      "odkFormID", "odkLastRun", "odkUseCentral",
                      "odkProjectNumber"]
        results = self.test_db.get_fields("ODK_Conf")
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
        os.remove("test_conf_pipeline.db")


if __name__ == "__main__":
    unittest.main(verbosity=2)
