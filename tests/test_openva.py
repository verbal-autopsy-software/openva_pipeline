from openva_pipeline.transfer_db import TransferDB
from openva_pipeline.pipeline import Pipeline
from openva_pipeline.openva import OpenVA
from openva_pipeline.run_pipeline import download_smartva
from openva_pipeline.run_pipeline import create_transfer_db
from openva_pipeline.exceptions import OpenVAError
from openva_pipeline.exceptions import SmartVAError
import unittest
import os
import shutil
import collections
from datetime import datetime
from sys import path, platform
from pandas import read_csv

source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class CheckCopyVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/previous_export.csv",
                    "ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/another_export.csv",
                    "ODKFiles/odk_export_new.csv")
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

        pl = Pipeline(db_file_name="Pipeline.db",
                      db_directory=".",
                      db_key="enilepiP")
        cls.static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")

        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )
        r_openva = OpenVA(pl.settings, cls.static_run_date)
        cls.summary = r_openva.prep_va_data()

    def test_copy_va_zero_records_false(self):
        """Check that prep_va_data() actually prepares VA records."""

        self.assertTrue(all(v > 0 for v in self.summary.values()))

    def test_copy_va_isfile(self):
        """Check that prep_va_data() brings in new file."""

        self.assertTrue(
            os.path.isfile("OpenVAFiles/openva_input.csv")
        )

    def test_copy_va_merge(self):
        """Check that prep_va_data() includes all records."""

        # has_all = True
        # with open("OpenVAFiles/pycrossva_input.csv") as f_combined:
        #     f_combined_lines = f_combined.readlines()
        # with open("ODKFiles/previous_export.csv") as f_previous:
        #     f_previous_lines = f_previous.readlines()
        # with open("ODKFiles/another_export.csv") as f_another:
        #     f_another_lines = f_another.readlines()
        # for line in f_previous_lines:
        #     if line not in f_combined_lines:
        #         has_all = False
        # for line in f_another_lines:
        #     if line not in f_combined_lines:
        #         has_all = False
        # self.assertTrue(has_all)
        combined = read_csv("OpenVAFiles/pycrossva_input.csv")
        previous = read_csv("ODKFiles/previous_export.csv")
        another = read_csv("ODKFiles/another_export.csv")
        n_match_previous = 0
        n_match_another = 0
        for i in range(previous.shape[0]):
            for j in range(combined.shape[0]):
                if previous.iloc[i].equals(combined.iloc[j]):
                    n_match_previous += 1
                    break
        for i in range(another.shape[0]):
            for j in range(combined.shape[0]):
                if another.iloc[i].equals(combined.iloc[j]):
                    n_match_another += 1
                    break
        has_all = (n_match_another == another.shape[0] and
                   n_match_previous == previous.shape[0])
        self.assertTrue(has_all)

    @classmethod
    def tearDownClass(cls):

        os.remove("ODKFiles/odk_export_prev.csv")
        os.remove("ODKFiles/odk_export_new.csv")
        os.remove("OpenVAFiles/openva_input.csv")
        os.remove("Pipeline.db")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )


class CheckZeroRecords(unittest.TestCase):

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
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

        pl = Pipeline(db_file_name="Pipeline.db",
                      db_directory=".",
                      db_key="enilepiP")
        cls.static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")

        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )

        r_openva = OpenVA(pl.settings, cls.static_run_date)
        cls.summary = r_openva.prep_va_data()

    def test_copy_va_zero_records_true(self):
        """Check that prep_va_data() properly handles empty data files."""

        self.assertTrue(all(v == 0 for v in self.summary.values()))

    def test_copy_va_zero_records_true_no_file(self):
        """Check that prep_va_data() does not produce file if zero records."""

        self.assertFalse(
            os.path.isfile("OpenVAFiles/openva_input.csv")
        )

    @classmethod
    def tearDownClass(cls):

        os.remove("ODKFiles/odk_export_prev.csv")
        os.remove("ODKFiles/odk_export_new.csv")
        os.remove("Pipeline.db")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )


class CheckInSilicoVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/previous_export.csv",
                    "ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/another_export.csv",
                    "ODKFiles/odk_export_new.csv")
        if os.path.isfile("Check_InSilicoVA_Pipeline.db"):
            os.remove("Check_InSilicoVA_Pipeline.db")
        create_transfer_db("Check_InSilicoVA_Pipeline.db", ".", "enilepiP")

        pl = Pipeline(db_file_name="Check_InSilicoVA_Pipeline.db",
                      db_directory=".",
                      db_key="enilepiP")
        fields = ["algorithm", "algorithmMetadataCode"]
        values = ["InSilicoVA",
                  ("InSilicoVA-2016|1.0.0|InterVA|5|"
                   "2016 WHO Verbal Autopsy Form|v1_4_1")]
        pl._update_pipeline(fields, values)
        pl.pipeline_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        pl._config()
        cls.static_run_date = \
            datetime(2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")

        cls.r_script = os.path.join("OpenVAFiles", cls.static_run_date,
                                    "r_script_" + cls.static_run_date + ".R")
        cls.r_out_file = os.path.join("OpenVAFiles", cls.static_run_date,
                                      "r_script_" + cls.static_run_date +
                                      ".Rout")
        r_openva = OpenVA(pl.settings, cls.static_run_date)
        r_openva.prep_va_data()
        r_openva.r_script()
        cls.completed = r_openva.get_cod()

    def test_insilicova_r_script(self):
        """Check that r_script() creates an R script for InSilicoVA."""

        self.assertTrue(os.path.isfile(self.r_script))

    def test_insilicova_r_out(self):
        """Check that get_cod() executes R script for InSilicoVA"""

        self.assertTrue(os.path.isfile(self.r_out_file))

    def test_insilicova_eav(self):
        """Check that get_cod() creates EAV output for InSilicoVA"""

        self.assertTrue(
            os.path.isfile("OpenVAFiles/entity_attribute_value.csv"))

    def test_insilicova_record_storage(self):
        """Check that get_cod() record_storage.csv for InSilicoVA"""

        self.assertTrue(os.path.isfile("OpenVAFiles/record_storage.csv"))

    def test_insilicova_return_code(self):
        """Check that get_cod() runs InSilicoVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")
        os.remove("Check_InSilicoVA_Pipeline.db")


class CheckInterVA(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/previous_export.csv",
                    "ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/another_export.csv",
                    "ODKFiles/odk_export_new.csv")
        if os.path.isfile("Check_InterVA_Pipeline.db"):
            os.remove("Check_InterVA_Pipeline.db")
        create_transfer_db("Check_InterVA_Pipeline.db", ".", "enilepiP")

        # pipeline_run_date = datetime.now()
        pipeline_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="Check_InterVA_Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date=pipeline_run_date)
        par = ["InterVA",
               "InterVA5|5|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1"]
        xfer_db.update_table("Pipeline_Conf",
                             ["algorithm", "algorithmMetadataCode"],
                             par)

        settings_pipeline = xfer_db.config_pipeline()
        settings_odk = xfer_db.config_odk()
        settings_interva = xfer_db.config_openva("InterVA")
        settings = {"odk": settings_odk,
                    "pipeline": settings_pipeline,
                    "openva": settings_interva}
        cls.static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )
        cls.r_script = os.path.join("OpenVAFiles", cls.static_run_date,
                                    "r_script_" + cls.static_run_date + ".R")
        cls.r_out_file = os.path.join("OpenVAFiles", cls.static_run_date,
                                      "r_script_" + cls.static_run_date +
                                      ".Rout")
        r_openva = OpenVA(settings=settings,
                          pipeline_run_date=cls.static_run_date)
        r_openva.prep_va_data()
        r_openva.r_script()
        cls.completed = r_openva.get_cod()

    def test_interva_rscript(self):
        """Check that get_cod() executes R script for InterVA"""

        self.assertTrue(os.path.isfile(self.r_script))

    def test_interva_rOut(self):
        """Check that get_cod() executes R script for InterVA"""

        self.assertTrue(os.path.isfile(self.r_out_file))

    def test_interva_EAV(self):
        """Check that get_cod() creates EAV outpue for InterVA"""

        self.assertTrue(
            os.path.isfile("OpenVAFiles/entity_attribute_value.csv"))

    def test_interva_record_storage(self):
        """Check that get_cod() record_storage.csv for InterVA"""

        self.assertTrue(os.path.isfile("OpenVAFiles/record_storage.csv"))

    def test_interva_return_code(self):
        """Check that get_cod() runs InterVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        os.remove("Check_InterVA_Pipeline.db")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


class CheckInterVAOrgUnit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_org_unit_in_id10057.csv",
                    "ODKFiles/odk_export_new.csv")
        if os.path.isfile("Check_InterVA_Pipeline.db"):
            os.remove("Check_InterVA_Pipeline.db")
        create_transfer_db("Check_InterVA_Pipeline.db", ".", "enilepiP")

        cls.pl = Pipeline(db_file_name="Check_InterVA_Pipeline.db",
                          db_directory=".",
                          db_key="enilepiP")
        cls.pl._update_dhis(['dhisOrgUnit'], ['Id10057'])
        r_openva = OpenVA(settings=cls.pl.settings,
                          pipeline_run_date=cls.pl.pipeline_run_date)
        r_openva.prep_va_data()
        r_openva.r_script()
        r_openva.get_cod()

    def test_record_storage_includes_org_units(self):
        """Check that output csv file includes DHIS organization units"""

        data = read_csv("ODKFiles/odk_export_org_unit_in_id10057.csv")
        org_units = data.filter(like="Id10057", axis=1).iloc[:, 0].tolist()
        results = read_csv("OpenVAFiles/record_storage.csv")
        results_org_units = results['org_unit_col1'].tolist()
        s1 = set(org_units)
        s2 = set(results_org_units)
        self.assertTrue(s1 == s2)

    @classmethod
    def tearDownClass(cls):

        os.remove("Check_InterVA_Pipeline.db")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.pl.pipeline_run_date),
            ignore_errors=True
        )
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


class CheckInSilicoVAOrgUnit(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        shutil.copy("ODKFiles/odk_export_org_unit_in_id10057.csv",
                    "ODKFiles/odk_export_new.csv")
        if os.path.isfile("Check_InterVA_Pipeline.db"):
            os.remove("Check_InterVA_Pipeline.db")
        create_transfer_db("Check_InSilicoVA_Pipeline.db", ".", "enilepiP")

        cls.pl = Pipeline(db_file_name="Check_InSilicoVA_Pipeline.db",
                          db_directory=".",
                          db_key="enilepiP")
        cls.pl._update_dhis(["dhisOrgUnit"], ["Id10057"])
        cls.pl._update_pipeline(["algorithm"], ["InSilicoVA"])
        r_openva = OpenVA(settings=cls.pl.settings,
                          pipeline_run_date=cls.pl.pipeline_run_date)
        r_openva.prep_va_data()
        r_openva.r_script()
        completed = r_openva.get_cod()

    def test_record_storage_includes_org_units(self):
        """Check that output csv file includes DHIS organization units"""

        data = read_csv("ODKFiles/odk_export_org_unit_in_id10057.csv")
        org_units = data.filter(like="Id10057", axis=1).iloc[:, 0].tolist()
        results = read_csv("OpenVAFiles/record_storage.csv")
        results_org_units = results['org_unit_col1'].tolist()
        s1 = set(org_units)
        s2 = set(results_org_units)
        self.assertTrue(s1 == s2)

    @classmethod
    def tearDownClass(cls):

        os.remove("Check_InSilicoVA_Pipeline.db")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.pl.pipeline_run_date),
            ignore_errors=True
        )
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


@unittest.skipIf(platform == "darwin", "Can't run smartva on MacOS")
class CheckSmartVA(unittest.TestCase):

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
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")

        # pipeline_run_date = datetime.now()
        pipeline_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date=pipeline_run_date)
        par = ["SmartVA", "SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1"]
        xfer_db.update_table("Pipeline_Conf",
                             ["algorithm", "algorithmMetadataCode"],
                             par)
        settings_pipeline = xfer_db.config_pipeline()
        settings_odk = xfer_db.config_odk()
        settings_smartva = xfer_db.config_openva("SmartVA")
        settings = {"odk": settings_odk,
                    "pipeline": settings_pipeline,
                    "openva": settings_smartva}
        cls.static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )
        cli_smartva = OpenVA(settings=settings,
                             pipeline_run_date=cls.static_run_date)
        cli_smartva.prep_va_data()
        cls.completed = cli_smartva.get_cod()
        cls.svaOut = os.path.join(
            "OpenVAFiles",
            cls.static_run_date,
            "1-individual-cause-of-death/individual-cause-of-death.csv"
        )

    def test_smartva(self):
        """Check that get_cod() executes smartva cli"""

        self.assertTrue(os.path.isfile(self.svaOut))

    def test_smartva_EAV(self):
        """Check that get_cod() creates EAV output for SmartVA"""

        self.assertTrue(
            os.path.isfile("OpenVAFiles/entity_attribute_value.csv"))

    def test_smartva_record_storage(self):
        """Check that get_cod() record_storage.csv for SmartVA"""

        self.assertTrue(os.path.isfile("OpenVAFiles/record_storage.csv"))

    def test_smartva_returncode(self):
        """Check that get_cod() runs SmartVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        os.remove("Pipeline.db")
        shutil.rmtree(
            os.path.join("OpenVAFiles", cls.static_run_date),
            ignore_errors=True
        )
        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


class CheckExceptionsInSilicoVA(unittest.TestCase):

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

    def setUp(self):

        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date=static_run_date)
        par = ["InSilicoVA",
               "InSilicoVA|1.1.4|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1"]
        xfer_db.update_table("Pipeline_Conf",
                             ["algorithm", "algorithmMetadataCode"],
                             par)
        xfer_db.update_table("InSilicoVA_Conf", "data_type", "WHO2012")
        settings_pipeline = xfer_db.config_pipeline()
        settings_odk = xfer_db.config_odk()
        settings_algorithm = xfer_db.config_openva("InSilicoVA")
        settings = {"odk": settings_odk,
                    "pipeline": settings_pipeline,
                    "openva": settings_algorithm}
        self.r_openva = OpenVA(settings=settings,
                               pipeline_run_date=static_run_date)
        self.r_openva.prep_va_data()
        self.r_openva.r_script()

    def test_insilico_exception(self):
        """get_cod() raises exception with faulty R script for InSilicoVA."""

        self.assertRaises(OpenVAError, self.r_openva.get_cod)

    def tearDown(self):
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join("OpenVAFiles", static_run_date),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


class CheckExceptionsInterVA(unittest.TestCase):

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

    def setUp(self):

        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date=static_run_date)
        par = ["InterVA",
               "InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1"]
        xfer_db.update_table("Pipeline_Conf",
                             ["algorithm", "algorithmMetadataCode"],
                             par)
        xfer_db.update_table("InterVA_Conf", "version", "4")
        settings_pipeline = xfer_db.config_pipeline()
        settings_algorithm = xfer_db.config_openva("InterVA")
        settings_odk = xfer_db.config_odk()
        settings = {"odk": settings_odk,
                    "pipeline": settings_pipeline,
                    "openva": settings_algorithm}

        self.r_openva = OpenVA(settings=settings,
                               pipeline_run_date=static_run_date)
        self.r_openva.prep_va_data()
        self.r_openva.r_script()

    def test_interva_exception(self):
        """get_cod() should raise an exception with problematic Interva R
        script."""

        self.assertRaises(OpenVAError, self.r_openva.get_cod)

    def tearDown(self):
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join("OpenVAFiles", static_run_date),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


@unittest.skipIf(platform == "darwin", "Can't run smartva on MacOS")
class CheckExceptionsSmartVA(unittest.TestCase):

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

    def setUp(self):

        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="copy_Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date=static_run_date)
        par = ["SmartVA", "SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1"]
        xfer_db.update_table("Pipeline_Conf",
                             ["algorithm", "algorithmMetadataCode"],
                             par)
        nt_smartva = collections.namedtuple("nt_smartva",
                                            ["smartva_country",
                                             "smartva_hiv",
                                             "smartva_malaria",
                                             "smartva_hce",
                                             "smartva_freetext",
                                             "smartva_figures",
                                             "smartva_language"])
        settings_algorithm = nt_smartva("Unknown",
                                        "Wrong",
                                        "Wrong",
                                        "Wrong",
                                        "Wrong",
                                        "Wrong",
                                        "Wrong")
        settings_pipeline = xfer_db.config_pipeline()
        settings_odk = xfer_db.config_odk()
        settings = {"odk": settings_odk,
                    "pipeline": settings_pipeline,
                    "openva": settings_algorithm}
        self.r_openva = OpenVA(settings=settings,
                               pipeline_run_date=static_run_date)
        self.r_openva.prep_va_data()

    def test_smartva_exception(self):
        """get_cod() should raise an exception with faulty args for
        smartva cli"""

        self.assertRaises(SmartVAError, self.r_openva.get_cod)

    def tearDown(self):
        static_run_date = datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        shutil.rmtree(
            os.path.join("OpenVAFiles", static_run_date),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile("ODKFiles/odk_export_new.csv"):
            os.remove("ODKFiles/odk_export_new.csv")
        if os.path.isfile("ODKFiles/odk_export_prev.csv"):
            os.remove("ODKFiles/odk_export_prev.csv")
        if os.path.isfile("OpenVAFiles/openva_input.csv"):
            os.remove("OpenVAFiles/openva_input.csv")
        if os.path.isfile("OpenVAFiles/record_storage.csv"):
            os.remove("OpenVAFiles/record_storage.csv")
        if os.path.isfile("OpenVAFiles/entity_attribute_value.csv"):
            os.remove("OpenVAFiles/entity_attribute_value.csv")


if __name__ == "__main__":
    unittest.main(verbosity=2)
