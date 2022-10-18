from openva_pipeline import dhis
from openva_pipeline.transfer_db import TransferDB
from openva_pipeline.run_pipeline import create_transfer_db
from openva_pipeline.exceptions import DHISError

import datetime
import subprocess
import shutil
import os
import unittest
import collections
from pandas import read_csv
from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context

os.chdir(os.path.abspath(os.path.dirname(__file__)))


@unittest.skip("Only to run locally with local (single event) DHIS2 server")
class CheckDHIS(unittest.TestCase):
    """Check that everything works as it should."""

    @classmethod
    def setUpClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        shutil.copy("OpenVAFiles/sample_eav.csv",
                    "OpenVAFiles/entity_attribute_value.csv")
        shutil.copy("OpenVAFiles/sample_record_storage.csv",
                    "OpenVAFiles/record_storage.csv")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db(db_file_name, 
                               db_directory, 
                               db_key)
        pipeline_run_date = datetime.datetime.now()

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        dhis_url = "http://localhost:8080"
        if os.path.isfile("/.dockerenv"):
            dhis_url = "http://host.docker.internal:8080"
        xfer_db.update_table("DHIS_Conf",
                             ["dhisURL",
                              "dhisUser",
                              "dhisPassword"],
                             [dhis_url,
                              "admin",
                              "district"])
        settings_dhis = xfer_db.config_dhis("InSilicoVA")
        settings_pipeline = xfer_db.config_pipeline()

        cls.pipeline_dhis = dhis.DHIS(settings_dhis, ".")
        event_uids = cls.pipeline_dhis.api_dhis.get(
            endpoint="events",
            params={"program": f"{cls.pipeline_dhis.va_program_uid}",
                    "fields": "event"}).get("events")
        list_uids = [i["event"] for i in event_uids]
        for uid in list_uids:
            cls.pipeline_dhis.api_dhis._delete_event(uid)

        cls.post_log = cls.pipeline_dhis.post_va(xfer_db)
        if cls.pipeline_dhis.post_to_tracker:
            cls.pipeline_dhis.verify_tei_post(cls.post_log)
        else:
            cls.pipeline_dhis.verify_post(cls.post_log)

    def test_va_program_uid(self):
        """Verify VA program is installed."""

        if self.pipeline_dhis.post_to_tracker:
            self.assertEqual(self.pipeline_dhis.va_program_uid, "nrEVPTOQoze")
        else:
            self.assertEqual(self.pipeline_dhis.va_program_uid, "sv91bCroFFx")

    def test_post_va(self):
        """Post VA records to DHIS2."""

        check_log = "importSummaries" in self.post_log["response"].keys()
        self.assertTrue(check_log)

    def test_verify_post(self):
        """Verify VA records got posted to DHIS2."""

        df_new_storage = read_csv("OpenVAFiles/new_storage.csv")
        n_pushed = sum(df_new_storage["pipelineOutcome"] == "Pushed to DHIS2")
        self.assertEqual(n_pushed, self.pipeline_dhis.n_posted_events)

    def test_get_org_units_va_program(self):
        """Check method for getting organisation units in VA Program."""
        ou_dict = self.pipeline_dhis._get_org_units(va_program=True)
        ou_server = {"Country": "wEVB21sQaHu",
                     "DistrictA1": "SCVeBskgiK6",
                     "FacilityA11": "CXmC6GyG6GI",
                     "FacilityA12": "RoEPvkwiu7U",
                     "RegionA": "Sd8e3Yi0vZA"}
        self.assertDictEqual(ou_dict, ou_server)

    def test_get_org_units_all(self):
        """Check method for getting all organisation units."""
        ou_dict = self.pipeline_dhis._get_org_units(va_program=False)
        ou_server = {"Country": "wEVB21sQaHu",
                     "DistrictA1": "SCVeBskgiK6",
                     "FacilityA11": "CXmC6GyG6GI",
                     "FacilityA12": "RoEPvkwiu7U",
                     "RegionA": "Sd8e3Yi0vZA",
                     "Not_VA": "wM2eziZ8zJ7"}
        self.assertDictEqual(ou_dict, ou_server)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        os.remove("OpenVAFiles/entity_attribute_value.csv")
        os.remove("OpenVAFiles/new_storage.csv")
        os.remove("Pipeline.db")


class CheckDHISTracker(unittest.TestCase):
    """Check that everything works as it should."""

    @classmethod
    def setUpClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        shutil.copy("OpenVAFiles/sample_eav.csv",
                    "OpenVAFiles/entity_attribute_value.csv")
        shutil.copy("OpenVAFiles/sample_record_storage.csv",
                    "OpenVAFiles/record_storage.csv")
        db_file_name = "Pipeline.db"
        db_key = "enilepiP"
        db_directory = "."
        if not os.path.isfile("Pipeline.db"):
            create_transfer_db(db_file_name,
                               db_directory,
                               db_key)
        pipeline_run_date = datetime.datetime.now()

        xfer_db = TransferDB(db_file_name=db_file_name,
                             db_directory=db_directory,
                             db_key=db_key,
                             pl_run_date=pipeline_run_date)
        settings_dhis = xfer_db.config_dhis("InSilicoVA")
        settings_pipeline = xfer_db.config_pipeline()
        cls.pipeline_dhis = dhis.DHIS(settings_dhis, ".")
        cls.post_log = cls.pipeline_dhis.post_va(xfer_db)
        if cls.pipeline_dhis.post_to_tracker:
            cls.pipeline_dhis.verify_tei_post(cls.post_log)
        else:
            cls.pipeline_dhis.verify_post(cls.post_log)

    def test_va_program_uid(self):
        """Verify VA program is installed."""

        if self.pipeline_dhis.post_to_tracker:
            self.assertEqual(self.pipeline_dhis.va_program_uid, "nrEVPTOQoze")
        else:
            self.assertEqual(self.pipeline_dhis.va_program_uid, "sv91bCroFFx")

    def test_post_va(self):
        """Post VA records to DHIS2."""

        check_log = "importSummaries" in self.post_log["response"].keys()
        self.assertTrue(check_log)

    def test_verify_tei_post(self):
        """Verify tracked entity instances events got posted to DHIS2."""

        df_new_storage = read_csv("OpenVAFiles/new_storage.csv")
        n_pushed = sum(df_new_storage["pipelineOutcome"] == "Pushed to DHIS2")
        self.assertEqual(n_pushed, self.pipeline_dhis.n_posted_events)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)
        os.remove("OpenVAFiles/entity_attribute_value.csv")
        os.remove("OpenVAFiles/new_storage.csv")
        os.remove("Pipeline.db")


class CheckDHISGetCODCode(unittest.TestCase):
    """Check get_cod_code function."""

    @classmethod
    def setUpClass(cls):

        if not os.path.isfile("Pipeline.db"):
            create_transfer_db("Pipeline.db", ".", "enilepiP")
        if os.path.isfile("who_cod.R"):
            os.remove("who_cod.R")

        pipeline_run_date = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime("%Y_%m_%d_%H:%M:%S")
        xfer_db = TransferDB(db_file_name="Pipeline.db",
                             db_directory=".",
                             db_key="enilepiP",
                             pl_run_date=pipeline_run_date)
        cls.cod_who = xfer_db.config_dhis("InSilicoVA")
        cls.cod_tariff = xfer_db.config_dhis("SmartVA")

        with open("who_cod.R", "w", newline="") as f:
            f.write("data(causetextV5, package='InterVA5')\n")
            f.write("write.csv(causetextV5, file='who_cod.csv', row.names=FALSE)\n")
        r_args = ["R", "CMD", "BATCH",
                  "--no-save", "--no-restore", "who_cod.R"]
        subprocess.run(args=r_args,
                       stdin=subprocess.PIPE,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       check=True)
        who = read_csv("who_cod.csv", index_col=0)
        index_who_causes = [i for i in who.index if "b_" in i]
        cls.who_causes = who.loc[index_who_causes, who.columns[0]].tolist()

        # NEED TO UPDATE VA_COD_CODES_OPTIONSET.JSON (METADATA) FOR DHIS2_VA_PROGRAM 
        # url_tariff = "https://github.com/ihmeuw/SmartVA-Analyze/raw/master/smartva/data/icds.py"
        # r = requests.get(url_tariff)
        # tariff = "#" + r.text
        # with open("tariff_cod.py", "w", newline="") as db_file:
        #     #print(r.text, file=db_file)
        #     db_file.write("ADULT = "adult"\n")
        #     db_file.write("CHILD = "child"\n")
        #     db_file.write("NEONATE = "neonate"\n")
        #     print(tariff, file=db_file)
        # from tariff_cod import ICDS
        # tariff_adult = list(ICDS["adult"].keys())
        # tariff_child = list(ICDS["child"].keys())
        # tariff_neonate = list(ICDS["neonate"].keys())
        # cls.tariff_causes = tariff_adult + tariff_child + tariff_neonate

    def test_who_causes(self):
        """Test matching for WHO causes of death."""

        for i in self.who_causes:
            with self.subTest(i=i):
                self.assertIsNotNone(dhis.get_cod_code(self.cod_who[1], i))

    # def test_tariff_causes(self):
    #     """Test matching for Tariff causes of death."""

    #     for i in self.tariff_causes:
    #         with self.subTest(i=i):
    #             self.assertIsNotNone(
    #             dhis.get_cod_code(self.cod_tariff[1], i))

    @classmethod
    def tearDownClass(cls):

        os.remove("who_cod.csv")
        os.remove("who_cod.R")
        os.remove("who_cod.Rout")


class CheckDHISExceptions(unittest.TestCase):
    """Check that DHIS raises exceptions when it should."""

    # removing test (if org unit not found, then death is posted to root level)
    # def test_org_unit_exception(self):
    #     """Verify exception is raised with faulty input."""
    #
    #     dhis_url = "https://va30tr.swisstph-mis.ch"
    #     dhis_user = "va-demo"
    #     dhis_password = "VerbalAutopsy99!"
    #     dhis_org_unit = "wrong"
    #     ntDHIS = collections.namedtuple("ntDHIS",
    #                                     ["dhis_url",
    #                                      "dhis_user",
    #                                      "dhis_password",
    #                                      "dhis_org_unit",
    #                                      "dhis_cod_codes"]
    #     )
    #     bad_settings = ntDHIS(dhis_url,
    #                           dhis_user,
    #                           dhis_password,
    #                           dhis_org_unit,
    #                           "InSilicoVA")
    #     mock_cod = {"cause1": "code1", "cause2": "code2"}
    #     bad_input = [bad_settings, mock_cod]
    #     pipeline_dhis = dhis.DHIS(bad_input, ".")
    #     self.assertRaises(dhis.DHISError, pipeline_dhis.connect)

    def test_dhis_user_exception(self):
        """Verify exception is raised with faulty input."""

        dhis_url = "https://va30tr.swisstph-mis.ch"
        dhis_user = "wrong"
        dhis_password = "VerbalAutopsy99!"
        dhis_org_unit = "SCVeBskgiK6"
        ntDHIS = collections.namedtuple("ntDHIS",
                                        ["dhis_url",
                                         "dhis_user",
                                         "dhis_password",
                                         "dhis_org_unit",
                                         "dhis_post_root",
                                         "dhis_cod_codes"]
        )
        bad_settings = ntDHIS(dhis_url,
                              dhis_user,
                              dhis_password,
                              dhis_org_unit,
                              "False",
                              "InSilicoVA")
        mock_cod = {"cause1": "code1", "cause2": "code2"}
        bad_input = [bad_settings, mock_cod]

        self.assertRaises(DHISError, dhis.DHIS, bad_input, ".")

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree("DHIS/blobs/", ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
