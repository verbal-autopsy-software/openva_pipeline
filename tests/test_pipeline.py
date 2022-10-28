from openva_pipeline.transfer_db import TransferDB
from openva_pipeline.pipeline import Pipeline
from openva_pipeline.run_pipeline import download_briefcase
from openva_pipeline.run_pipeline import download_smartva
from openva_pipeline.run_pipeline import create_transfer_db
import datetime
import shutil
import os
import unittest
from pandas import read_csv

from sys import path, platform
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class CheckPipelineConfig(unittest.TestCase):
    """Check config method:"""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        pl = Pipeline("Pipeline.db", ".", "enilepiP", True)
        cls.settings_pipeline = pl.settings["pipeline"]
        cls.settings_odk = pl.settings["odk"]
        cls.settings_openva = pl.settings["openva"]
        cls.settings_dhis = pl.settings["dhis"]

    def test_config_pipeline_algorithm_metadata_code(self):
        """Test config method configuration of pipeline:
        settings_pipeline.algorithmMetadataCode:"""

        self.assertEqual(
            self.settings_pipeline.algorithm_metadata_code,
            "InterVA5|5|InterVA|5|2016 WHO Verbal Autopsy Form|v1_5_1")

    def test_config_pipeline_cod_source(self):
        """Test config method configuration of pipeline:
        settings_pipeline.codSource:"""

        self.assertEqual(self.settings_pipeline.cod_source, "WHO")

    def test_config_pipeline_algorithm(self):
        """Test config method configuration of pipeline:
        settings_pipeline.algorithm:"""

        self.assertEqual(self.settings_pipeline.algorithm, "InterVA")

    def test_config_pipeline_working_direcotry(self):
        """Test config method configuration of pipeline:
        settings_pipeline.working_directory:"""

        self.assertEqual(self.settings_pipeline.working_directory, ".")

    def test_config_odk_odk_id(self):
        """Test config method configuration of pipeline:
        settings_odk.odk_id:"""

        self.assertEqual(self.settings_odk.odk_id, None)

    def test_config_odk_odk_url(self):
        """Test config method configuration of pipeline:
        settings_odk.odk_url:"""

        self.assertEqual(self.settings_odk.odk_url,
                         "https://odk-central.swisstph.ch")

    def test_config_odk_odk_user(self):
        """Test config method configuration of pipeline:
        settings_odk.odk_user:"""

        self.assertEqual(self.settings_odk.odk_user, "who.va.view@swisstph.ch")

    def test_config_odk_odk_password(self):
        """Test config method configuration of pipeline:
        settings_odk.odk_password:"""

        self.assertEqual(self.settings_odk.odk_password, "WHOVAVi3w153!")

    def test_config_odk_odk_form_id(self):
        """Test config method configuration of pipeline:
        settings_odk.odk_form_id:"""

        self.assertEqual(self.settings_odk.odk_form_id,
                         "va_who_v1_5_3")

    def test_config_odk_odk_last_run(self):
        """Test config method configuration of pipeline:
        settings_odk.odk_last_run:"""

        self.assertEqual(self.settings_odk.odk_last_run, "1900-01-01_00:00:01")

    def test_config_odk_odk_use_central(self):
        """Test config method configuration of pipeline:
        settings_odk.odkUseCentral:"""

        self.assertEqual(self.settings_odk.odk_use_central, "True")

    def test_config_odk_odk_project_number(self):
        """Test config method configuration of pipeline:
        settings_odk.odk_project_number:"""

        self.assertEqual(self.settings_odk.odk_project_number, "40")

    def test_config_dhis_dhis_url(self):
        """Test config method configuration of pipeline:
        settings_dhis.dhis_url:"""

        self.assertEqual(self.settings_dhis[0].dhis_url,
                         "https://va30tr.swisstph-mis.ch")

    def test_config_dhis_dhis_user(self):
        """Test config method configuration of pipeline:
        settings_dhis.dhis_user:"""

        self.assertEqual(self.settings_dhis[0].dhis_user, "va-demo")

    def test_config_dhis_dhis_password(self):
        """Test config method configuration of pipeline:
        settings_dhis.dhis_password:"""

        self.assertEqual(self.settings_dhis[0].dhis_password, 
                         "VerbalAutopsy99!")

    def test_config_dhis_dhis_org_unit(self):
        """Test config method configuration of pipeline:
        settings_dhis.dhis_org_unit:"""

        self.assertEqual(self.settings_dhis[0].dhis_org_unit, "SCVeBskgiK6")

    @classmethod
    def tearDownClass(cls):

        os.remove("Pipeline.db")


class DownloadAppsTests(unittest.TestCase):
    """Check the methods for downloading external apps:"""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        cls.pl = Pipeline("Pipeline.db", ".", "enilepiP", True)

    def test_download_briefcase(self):
        """Check download_briefcase():"""

        if os.path.isfile("ODK-Briefcase-v1.18.0.jar"):
            os.remove("ODK-Briefcase-v1.18.0.jar")
        download_briefcase()
        self.assertTrue(os.path.isfile("ODK-Briefcase-v1.18.0.jar"))

    def test_download_smartva(self):
        """Check download_smartva():"""

        if os.path.isfile("smartva"):
            os.remove("smartva")
        download_smartva()
        self.assertTrue(os.path.isfile("smartva"))

    @classmethod
    def tearDownClass(cls):

        os.remove("Pipeline.db")


class CheckRunODKClean(unittest.TestCase):
    """Check run_odk method on initial run:"""

    @classmethod
    def setUpClass(cls):

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors=True)
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if not os.path.isfile("ODK-Briefcase-v1.18.0.jar"):
            download_briefcase()
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

        pl = Pipeline("Pipeline.db", ".", "enilepiP", True)
        cls.odk = pl.run_odk()

    def test_clean_run_odk_return_code(self):
        """Check return code with valid parameters:"""

        self.assertTrue("Downloaded" in self.odk[0])

    def test_clean_run_odk_creates_odk_export_new(self):
        """Check for exported CSV file:"""

        self.assertTrue(os.path.isfile("ODKFiles/odk_export_new.csv"))

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors=True)
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        os.remove("Pipeline.db")


class CheckRunODKWithExports(unittest.TestCase):
    """Check run_odk method with existing ODK exports:"""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

    def setUp(self):

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors=True)
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/previous_export.csv", 
                    "ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/another_export.csv", 
                    "ODKFiles/odk_export_new.csv")
        self.old_mtime_prev = os.path.getmtime("ODKFiles/odk_export_prev.csv")
        self.old_mtime_new = os.path.getmtime("ODKFiles/odk_export_new.csv")
        if not os.path.isfile("ODK-Briefcase-v1.18.0.jar"):
            download_briefcase()
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        self.db_file_name = "Pipeline.db"
        self.db_directory = "."
        self.db_key = "enilepiP"
        self.use_dhis = True
        self.pl = Pipeline(self.db_file_name, self.db_directory,
                           self.db_key, self.use_dhis)
        self.odk = self.pl.run_odk()
        self.new_mtime_prev = os.path.getmtime("ODKFiles/odk_export_prev.csv")
        self.new_mtime_new = os.path.getmtime("ODKFiles/odk_export_new.csv")

    def tearDown(self):

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors=True)
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")

    def test_run_odk_return_code_with_previous_exports(self):
        """Check return code with valid parameters:"""

        self.assertTrue("Downloaded" in self.odk[0])

    def test_run_odk_export_prev_with_previous_exports(self):
        """Check modification time on odk_export_prev with previous exports:"""

        self.assertTrue(self.new_mtime_prev > self.old_mtime_prev)

    def test_run_odk_export_new_with_previous_exports(self):
        """Check modification time on odk_export_new:"""

        self.assertTrue(self.new_mtime_new > self.old_mtime_new)

    def test_run_odk_merge_to_prev_export_with_previous_exports(self):
        """Check merge_to_prev_export() keeps all records from BC export 
        files:"""

        has_all = True
        with open("ODKFiles/odk_export_prev.csv") as f_combined:
            f_combined_lines = f_combined.readlines()
        with open("ODKFiles/previous_export.csv") as f_previous:
            f_previous_lines = f_previous.readlines()
        with open("ODKFiles/another_export.csv") as f_another:
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

        os.remove("Pipeline.db")


@unittest.skipIf(platform == "darwin", "Can't run smartva CLI on MacOS")
class CheckStoreResultsDB(unittest.TestCase):
    """Check store_results_db method marks duplicate records:"""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

    def setUp(self):

        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors=True)
        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if not os.path.isfile("ODK-Briefcase-v1.18.0.jar"):
            download_briefcase()
        self.pl = Pipeline("Pipeline.db", ".", "enilepiP", True)
        dhis_url = "http://localhost:8080"
        if os.path.isfile("/.dockerenv"):
            dhis_url = "http://host.docker.internal:8080"
        self.pl._update_dhis(["dhisURL", "dhisUser", "dhisPassword"],
                             [dhis_url, "admin", "district"])

        self.xfer_db = TransferDB(db_file_name="Pipeline.db",
                                  db_directory=".",
                                  db_key="enilepiP",
                                  pl_run_date=True)
        self.conn = self.xfer_db._connect_db()
        self.c = self.conn.cursor()
        self.c.execute("DELETE FROM EventLog;")
        self.conn.commit()
        self.c.execute("DELETE FROM VA_Storage;")
        self.conn.commit()
        self.odk = self.pl.run_odk()

    @unittest.skip("Only to run locally with local single event DHIS2 server")
    def test_run_odk_check_duplicates(self):
        """Check check_duplicates() method:"""

        va_records = read_csv("ODKFiles/odk_export_new.csv")
        n_va = va_records.shape[0]
        r_out = self.pl.run_openva()
        pipelineDHIS = self.pl.run_dhis()
        self.pl.store_results_db()
        os.remove("ODKFiles/odk_export_new.csv")
        os.remove("OpenVAFiles/pycrossva_input.csv")
        os.remove("OpenVAFiles/openva_input.csv")
        odk2 = self.pl.run_odk()
        self.c.execute("SELECT eventDesc FROM EventLog;")
        query = self.c.fetchall()
        n_duplicates = [i[0] for i in query if "duplicate" in i[0]]
        self.assertEqual(len(n_duplicates), n_va)

    def tearDown(self):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        shutil.rmtree("ODKFiles/ODK Briefcase Storage/", ignore_errors=True)
        self.conn.close()

    @classmethod
    def tearDownClass(cls):

        os.remove("Pipeline.db")


class CheckRunOpenVA(unittest.TestCase):
    """Check run_openva method sets up files correctly"""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_prev_who_v151.csv",
                    "ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_new_who_v151.csv",
                    "ODKFiles/odk_export_new.csv")
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

        pl = Pipeline("Pipeline.db", ".", "enilepiP", True)
        cls.r_out = pl.run_openva()

    def test_creates_openva_input_csv(self):
        """Check that run_openva() brings in new file:"""

        self.assertTrue(
            os.path.isfile("OpenVAFiles/openva_input.csv")
        )

    def test_merges_records(self):
        """Check that run_openva() includes all records:"""

        has_all = True
        # with open("OpenVAFiles/pycrossva_input.csv") as f_combined:
        #     f_combined_lines = f_combined.readlines()
        # with open("ODKFiles/odk_export_prev_who_v151.csv") as f_previous:
        #     f_previous_lines = f_previous.readlines()
        # with open("ODKFiles/odk_export_new_who_v151.csv") as f_another:
        #     f_another_lines = f_another.readlines()
        # for line in f_previous_lines:
        #     if line not in f_combined_lines:
        #         has_all = False
        # for line in f_another_lines:
        #     if line not in f_combined_lines:
        #         has_all = False
        combined = read_csv("OpenVAFiles/pycrossva_input.csv")
        combined_id = combined["meta-instanceID"].tolist()
        prev = read_csv("ODKFiles/odk_export_prev.csv")
        prev_id = prev["meta-instanceID"]
        new = read_csv("ODKFiles/odk_export_new.csv")
        new_id = prev["meta-instanceID"]

        for id in prev_id:
            if id not in combined_id:
                has_all = False
        for id in new_id:
            if id not in combined_id:
                has_all = False
        self.assertTrue(has_all)

    def test_n_processed_records(self):
        """Check that run_openva() returns n_processed > 0"""

        self.assertTrue(self.r_out["n_processed"] > 0)

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        os.remove("Pipeline.db")


class CheckRunOpenVAZeroRecords(unittest.TestCase):
    """Check run_openva method sets up files correctly"""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/zero_records_export.csv",
                    "ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/zero_records_export.csv",
                    "ODKFiles/odk_export_new.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")

        pl_zero = Pipeline("copy_Pipeline.db", ".", "enilepiP", True)
        cls.r_out = pl_zero.run_openva()

    def test_zero_records_true(self):
        """Check that run_openva() returns zero_records == True:"""

        self.assertTrue(self.r_out["n_processed"] == 0)

    def test_zero_records_no_input_csv(self):
        """Check that run_openva() doesn't create new file if zero_records:"""

        self.assertFalse(
            os.path.isfile("OpenVAFiles/openva_input.csv")
        )

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")


class CheckPipelineRunOpenVAInSilicoVA(unittest.TestCase):
    """Check run_openva method runs InSilicoVA"""

    @classmethod
    def setUpClass(cls):

        now_date = datetime.datetime.now()
        pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="copy_Pipeline.db", 
                             db_directory=".",
                             db_key="enilepiP", 
                             pl_run_date=pipeline_run_date)
        par = ["InSilicoVA", "InSilicoVA|1.1.4|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1"]
        xfer_db.update_table("Pipeline_Conf",
                             ["algorithm", "algorithmMetadataCode"],
                             par)
        xfer_db.update_table("InSilicoVA_Conf", "data_type", "WHO2016")
        cls.pl = Pipeline("copy_Pipeline.db", ".", "enilepiP", True)

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/previous_export.csv",
                    "ODKFiles/odk_export_new.csv")
        shutil.copy("ODKFiles/another_export.csv",
                    "ODKFiles/odk_export_new.csv")

        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")

        cls.r_out = cls.pl.run_openva()

    def test_run_openva_insilico_r(self):
        """Check that run_openva() creates an R script for InSilicoVA:"""

        r_script_file = os.path.join("OpenVAFiles",
                                     self.pl.pipeline_run_date,
                                     "r_script_" + self.pl.pipeline_run_date + ".R")
        self.assertTrue(os.path.isfile(r_script_file))

    def test_run_openva_insilico_rout(self):
        """Check that run_openva() runs R script for InSilicoVA:"""

        r_script_file = os.path.join("OpenVAFiles",
                                     self.pl.pipeline_run_date,
                                     "r_script_" + self.pl.pipeline_run_date + ".Rout")
        self.assertTrue(os.path.isfile(r_script_file))

    def test_run_openva_insilico_completed(self):
        """Check that run_openva() creates an R script for InSilicoVA:"""

        self.assertEqual(self.r_out["return_code"], 0)

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


class CheckPipelineRunOpenVAInterVA(unittest.TestCase):
    """Check run_openva method runs InterVA"""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("InterVA_Pipeline.db"):
            os.remove("InterVA_Pipeline.db")
        create_transfer_db("InterVA_Pipeline.db", ".", "enilepiP")
        now_date = datetime.datetime.now()
        pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="InterVA_Pipeline.db", 
                             db_directory=".",
                             db_key="enilepiP", 
                             pl_run_date= pipeline_run_date)
        par = ["InterVA", "InterVA5|5|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1"]
        xfer_db.update_table("Pipeline_Conf",
                             ["algorithm", "algorithmMetadataCode"],
                             par)
        cls.pl = Pipeline("copy_Pipeline.db", ".", "enilepiP", True)

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/previous_export.csv",
                    "ODKFiles/odk_export_new.csv")
        shutil.copy("ODKFiles/another_export.csv",
                    "ODKFiles/odk_export_new.csv")

        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")

        cls.r_out = cls.pl.run_openva()

    def test_run_openva_interva_r(self):
        """Check that run_openva() creates an R script for InterVA:"""

        r_script_file = os.path.join("OpenVAFiles",
                                     self.pl.pipeline_run_date,
                                     "r_script_" + self.pl.pipeline_run_date + ".R")
        self.assertTrue(os.path.isfile(r_script_file))

    def test_run_openva_interva_rout(self):
        """Check that run_openva() runs R script for InterVA:"""

        r_script_file = os.path.join("OpenVAFiles",
                                   self.pl.pipeline_run_date,
                                   "r_script_" + self.pl.pipeline_run_date + ".Rout")
        self.assertTrue(os.path.isfile(r_script_file))

    def test_run_openva_InterVA_completed(self):
        """Check that run_openva() creates an R script for InterVA:"""

        self.assertEqual(self.r_out["return_code"], 0)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("OpenVAFiles/" + cls.pl.pipeline_run_date,
                      ignore_errors=True)
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("InterVA_Pipeline.db"):
            os.remove("InterVA_Pipeline.db")


@unittest.skipIf(platform == "darwin", "Can't run smartva CLI on MacOS")
class CheckRunOpenVASmartVA(unittest.TestCase):
    """Check run_openva method runs SmartVA"""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_phmrc-1.csv",
                    "ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_phmrc-2.csv",
                    "ODKFiles/odk_export_new.csv")
        if not os.path.isfile("smartva"):
            download_smartva()

        now_date = datetime.datetime.now()
        cls.pl = Pipeline("copy_smartVA_Pipeline.db", ".", "enilepiP", True)

        cls.r_out = cls.pl.run_openva()
        cls.sva_out = os.path.join(
            "OpenVAFiles",
            cls.pl.pipeline_run_date,
            "1-individual-cause-of-death/individual-cause-of-death.csv"
        )

    def test_run_openva_smartva_results(self):
        """Check that run_openva() executes SmartVA cli"""

        self.assertTrue(os.path.isfile(self.sva_out))

    def test_run_openva_smartva_eva(self):
        """Check that run_openva() creates EAV output for SmartVA """

        self.assertTrue(
            os.path.isfile("OpenVAFiles/entity_attribute_value.csv"))

    def test_run_openva_smartva_record_storage(self):
        """Check that run_openva() creates record_storage.csv for SmartVA"""

        self.assertTrue(os.path.isfile("OpenVAFiles/record_storage.csv"))

    def test_run_openva_smartva_completed(self):
        """Check run_openva() return_code for SmartVA:"""

        self.assertEqual(self.r_out["return_code"], 0)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.pl.pipeline_run_date),
            ignore_errors=True
        )
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
 

@unittest.skip("Only to run locally with local (single event) DHIS2 server")
class CheckPipelineRunDHIS(unittest.TestCase):
    """Check run_dhis method"""

    @classmethod
    def setUpClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        shutil.copy("OpenVAFiles/sample_eav.csv",
                    "OpenVAFiles/entity_attribute_value.csv")
        shutil.copy("OpenVAFiles/sample_record_storage.csv",
                    "OpenVAFiles/record_storage.csv")
        shutil.copy("OpenVAFiles/sample_new_storage.csv",
                    "OpenVAFiles/new_storage.csv")
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

        pl = Pipeline("Pipeline.db", ".", "enilepiP", True)
        dhis_url = "http://localhost:8080"
        if os.path.isfile("/.dockerenv"):
            dhis_url = "http://host.docker.internal:8080"
        pl._update_dhis(["dhisURL", "dhisUser", "dhisPassword"],
                        [dhis_url, "admin", "district"])
        cls.pipeline_dhis = pl.run_dhis()

    def test_run_dhis_va_program_uid(self):
        """Verify VA program is installed:"""

        self.assertEqual(self.pipeline_dhis["va_program_uid"], "sv91bCroFFx")

    def test_run_dhis_post_va(self):
        """Post VA records to DHIS2:"""

        post_log = self.pipeline_dhis["post_log"]
        check_log = "importSummaries" in post_log["response"].keys()
        self.assertTrue(check_log)

    def test_run_dhis_verify_post(self):
        """Verify VA records got posted to DHIS2:"""

        df_new_storage = read_csv("OpenVAFiles/new_storage.csv")
        n_pushed = sum(df_new_storage["pipelineOutcome"] == "Pushed to DHIS2")
        self.assertEqual(n_pushed, self.pipeline_dhis["n_posted_events"])

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/new_storage.csv"):
            os.remove("OpenVAFiles/new_storage.csv")
        os.remove("Pipeline.db")


class CheckPipelineDepositResults(unittest.TestCase):
    """Store VA results in Transfer database."""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        if os.path.isfile("OpenVAFiles/new_storage.csv"):
            os.remove("OpenVAFiles/new_storage.csv")
        shutil.copy("OpenVAFiles/sample_new_storage_verified.csv",
                    "OpenVAFiles/new_storage.csv")
        now_date = datetime.datetime.now()
        pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date=pipeline_run_date)
        conn = xfer_db._connect_db()
        c = conn.cursor()
        c.execute("DELETE FROM VA_Storage;")
        conn.commit()
        #conn.close()
        pl = Pipeline("Pipeline.db", ".", "enilepiP", True)
        pl._connect_dhis()
        pl.store_results_db()

        # xfer_db = TransferDB(db_file_name="Pipeline.db",
        #                      db_directory=".",
        #                      db_key="enilepiP",
        #                      pl_run_date=pipeline_run_date)
        conn = xfer_db._connect_db()
        c = conn.cursor()
        sql = "SELECT id FROM VA_Storage"
        c.execute(sql)
        va_ids = c.fetchall()
        conn.close()
        va_ids_list = [j for i in va_ids for j in i]
        cls.s1 = set(va_ids_list)
        df_new_storage = read_csv("OpenVAFiles/new_storage.csv")
        df_new_storage_id = df_new_storage["odkMetaInstanceID"]
        cls.s2 = set(df_new_storage_id)

    def test_store_va(self):
        """Check that depositResults() stores VA records in Transfer DB:"""

        self.assertTrue(self.s2.issubset(self.s1))

    @classmethod
    def tearDownClass(cls):
        if os.path.isfile("OpenVAFiles/new_storage.csv"):
            os.remove("OpenVAFiles/new_storage.csv")
        os.remove("Pipeline.db")


class CheckPipelineOrgUnitsTEI(unittest.TestCase):
    """Check run_dhis method with VAs sent to different DHIS2 organisation
    units for tracker version."""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_org_unit_in_id10057_tei.csv",
                    "ODKFiles/odk_export_new.csv")
        if os.path.isfile("org_units.db"):
            os.remove("org_units.db")
        create_transfer_db("org_units.db", ".", "enilepiP")

        cls.pl = Pipeline(db_file_name="org_units.db",
                          db_directory=".",
                          db_key="enilepiP")
        cls.pl._update_dhis(['dhisOrgUnit'], ['Id10057'])
        cls.pl.run_openva()
        cls.pipeline_dhis = cls.pl.run_dhis()
        cls.post_log = cls.pipeline_dhis["post_log"]
        odk_data = read_csv("ODKFiles/odk_export_new.csv")
        # Note: on server the only valid org unit is "DistrictA1"
        cls.n_no_org_unit = odk_data[
            odk_data.filter(like="10057").iloc[:, 0] != "DistrictA1"].shape[0]
        cls.pre_fix_va_storage_ids = cls.pl.xfer_db._get_va_storage_ids()
        cls.no_ou_vas = cls.pl.get_no_org_unit()
        cls.fix_va_id = next(iter(cls.no_ou_vas))
        cls.log = cls.pl.fix_no_org_unit(va_id=cls.fix_va_id,
                                         org_unit="DistrictA1")
        # TODO: finish tests (with test for 0 records to post b/c no org unit)

    def test_check_ou_not_found(self):
        """Check for records in VA_Org_Unit_Not_Found table."""
        assert(len(self.no_ou_vas) == self.n_no_org_unit)

    def test_check_instance_attribute_ou_not_found(self):
        """Check for DHIS instance attribute for number of records with
        invalid org units."""
        assert(self.pipeline_dhis["n_no_valid_org_unit"] == self.n_no_org_unit)

    def test_check_fix_out(self):
        """Check that events with fixed org unit codes get posted to DHIS.
        """
        self.assertTrue(self.log["event_status"] == "SUCCESS")

    def test_check_fixed_ou_not_found(self):
        """Check that events with fixed org unit codes are removed from the
        VA_Org_Unit_Not_Found table.
        """
        new_no_ou_vas = self.pl.get_no_org_unit()
        self.assertFalse(self.fix_va_id in new_no_ou_vas.keys())

    def test_pre_store_single_va(self):
        """Check that VA record is NOT stored in VA_Storage before fix."""
        self.assertFalse(self.fix_va_id in self.pre_fix_va_storage_ids)

    def test_post_store_single_va(self):
        """Check that VA record IS stored in VA_Storage after fix."""
        post_fix_va_storage_ids = self.pl.xfer_db._get_va_storage_ids()
        self.assertTrue(self.fix_va_id in post_fix_va_storage_ids)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/new_storage.csv"):
            os.remove("OpenVAFiles/new_storage.csv")
        os.remove("org_units.db")


@unittest.skip("Only to run locally with local (single event) DHIS2 server")
class CheckPipelineOrgUnits(unittest.TestCase):
    """Check run_dhis method with VAs sent to different DHIS2 organisation
    units."""

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_org_unit_in_id10057.csv",
                    "ODKFiles/odk_export_new.csv")
        if os.path.isfile("org_units.db"):
            os.remove("org_units.db")
        create_transfer_db("org_units.db", ".", "enilepiP")

        cls.pl = Pipeline(db_file_name="org_units.db",
                          db_directory=".",
                          db_key="enilepiP")
        dhis_url = "http://localhost:8080"
        if os.path.isfile("/.dockerenv"):
            dhis_url = "http://host.docker.internal:8080"
        cls.pl._update_dhis(["dhisURL", "dhisUser", "dhisPassword"],
                            [dhis_url, "admin", "district"])
        cls.pl._update_dhis(['dhisOrgUnit'], ['Id10057'])
        cls.pl.run_openva()
        cls.pipeline_dhis = cls.pl.run_dhis()
        cls.post_log = cls.pipeline_dhis["post_log"]
        odk_data = read_csv("ODKFiles/odk_export_new.csv")
        ou_cond = odk_data.filter(like="10057").iloc[:, 0].fillna("FacilityA13")
        cls.n_no_org_unit = odk_data[ou_cond == "FacilityA13"].shape[0]
        cls.pre_fix_va_storage_ids = cls.pl.xfer_db._get_va_storage_ids()
        cls.no_ou_vas = cls.pl.get_no_org_unit()
        cls.fix_va_id = next(iter(cls.no_ou_vas))
        cls.log = cls.pl.fix_no_org_unit(va_id=cls.fix_va_id,
                                         org_unit="DistrictA1")
        # TODO: finish tests (with test for 0 records to post b/c no org unit)

    def test_check_ou_not_found(self):
        """Check for records in VA_Org_Unit_Not_Found table."""
        assert(len(self.no_ou_vas) == self.n_no_org_unit)

    def test_check_instance_attribute_ou_not_found(self):
        """Check for DHIS instance attribute for number of records with
        invalid org units."""
        assert(self.pipeline_dhis["n_no_valid_org_unit"] == self.n_no_org_unit)

    def test_check_fix_out(self):
        """Check that events with fixed org unit codes get posted to DHIS.
        """
        self.assertTrue(self.log["event_status"] == "SUCCESS")

    def test_check_fixed_ou_not_found(self):
        """Check that events with fixed org unit codes are removed from the
        VA_Org_Unit_Not_Found table.
        """
        new_no_ou_vas = self.pl.get_no_org_unit()
        self.assertFalse(self.fix_va_id in new_no_ou_vas.keys())

    def test_pre_store_single_va(self):
        """Check that VA record is NOT stored in VA_Storage before fix."""
        self.assertFalse(self.fix_va_id in self.pre_fix_va_storage_ids)

    def test_post_store_single_va(self):
        """Check that VA record IS stored in VA_Storage after fix."""
        post_fix_va_storage_ids = self.pl.xfer_db._get_va_storage_ids()
        self.assertTrue(self.fix_va_id in post_fix_va_storage_ids)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/new_storage.csv"):
            os.remove("OpenVAFiles/new_storage.csv")
        os.remove("org_units.db")


class CheckPipelineCleanPipeline(unittest.TestCase):
    """Update ODK_Conf ODKLastRun in Transfer DB and clean up files."""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("ODKFiles/odk_export_new.csv"):
            shutil.copy("ODKFiles/previous_export.csv",
                        "ODKFiles/odk_export_prev.csv")
        if not os.path.isfile("ODKFiles/odk_export_prev.csv"):
            shutil.copy("ODKFiles/another_export.csv",
                        "ODKFiles/odk_export_new.csv")

        if not os.path.isfile("OpenVAFiles/openva_input.csv"):
            shutil.copy("OpenVAFiles/sample_openva_input.csv",
                        "OpenVAFiles/openva_input.csv")
        if not os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            shutil.copy("OpenVAFiles/sample_eav.csv",
                        "OpenVAFiles/entity_attribute_value.csv")
        if not os.path.isfile("OpenVAFiles/record_storage.csv"):
            shutil.copy("OpenVAFiles/sample_record_storage.csv",
                        "OpenVAFiles/record_storage.csv")
        if not os.path.isfile("OpenVAFiles/new_storage.csv"):
            shutil.copy("OpenVAFiles/sample_new_storage.csv",
                        "OpenVAFiles/new_storage.csv")

        os.makedirs("DHIS/blobs/", exist_ok = True)
        shutil.copy("OpenVAFiles/sample_new_storage.csv",
                    "DHIS/blobs/001-002-003.db")

        now_date = datetime.datetime.now()
        pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        cls.pl = Pipeline("copy_Pipeline.db", ".", "enilepiP", True)
        cls.pl.close_pipeline()

        xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date= pipeline_run_date)
        cls.conn = xfer_db._connect_db()
        cls.c = cls.conn.cursor()

    def test_clean_pipeline_rm_files(self):
        """Test file removal:"""

        file_exist = False
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            file_exist = True
            print("Problem: found ODKFiles/odk_export_new.csv \n")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            file_exist = True
            print("Problem: found ODKFiles/odk_export_prev.csv \n")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            file_exist = True
            print("Problem: found OpenVAFiles/openva_input.csv \n")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            file_exist = True
            print("Problem: found OpenVAFiles/entity_attribute_value.csv \n")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            file_exist = True
            print("Problem: found OpenVAFiles/record_storage.csv \n")
        if os.path.isfile("OpenVAFiles/new_storage.csv"):
            file_exist = True
        if os.path.isfile("DHIS/blobs/001-002-003.db"):
            file_exist = True
        self.assertFalse(file_exist)

    def test_clean_pipeline_odk_last_run(self):
        """Test update of ODK_Conf.odk_last_run:"""

        self.c.execute("SELECT odkLastRun FROM ODK_Conf;")
        sql_query = self.c.fetchone()
        results = [i for i in sql_query]
        self.assertEqual(results[0], self.pl.pipeline_run_date)

    @classmethod
    def tearDownClass(cls):

        cls.c.execute(
            "UPDATE ODK_Conf SET odkLastRun = '1900-01-01_00:00:01';")
        cls.conn.commit()
        cls.conn.close()
        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        if os.path.isfile("OpenVAFiles/new_storage.csv"):
            os.remove("OpenVAFiles/new_storage.csv")


if __name__ == "__main__":
    unittest.main(verbosity=2)
