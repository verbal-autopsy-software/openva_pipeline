import datetime
import subprocess
import shutil
import os
import unittest
import collections
import requests
from pandas import read_csv
from pysqlcipher3 import dbapi2 as sqlcipher

from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context
from openva_pipeline import dhis
from openva_pipeline.transfer_db import TransferDB
from openva_pipeline.runPipeline import createTransferDB

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class Check_DHIS(unittest.TestCase):
    """Check that everything works as it should."""


    @classmethod
    def setUpClass(cls):

        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        shutil.copy('OpenVAFiles/sampleEAV.csv',
                    'OpenVAFiles/entityAttributeValue.csv')
        shutil.copy('OpenVAFiles/sample_recordStorage.csv',
                    'OpenVAFiles/recordStorage.csv')
        # Define valid parameters for SwissTPH DHIS2 Server.
        dirOpenVA = 'OpenVAFiles'
        dhisURL = 'https://va30se.swisstph-mis.ch'
        # dhisURL = 'https://va25.swisstph-mis.ch'
        dhisUser = 'va-demo'
        dhisPassword = 'VerbalAutopsy99!'
        dhisOrgUnit = 'SCVeBskgiK6'

        # parameters for connecting to DB (assuming DB is in tests folder)
        dbFileName = 'Pipeline.db'
        dbKey = 'enilepiP'
        wrong_dbKey = 'wrongKey'
        # db_directory = os.path.abspath(os.path.dirname(__file__))
        dbDirectory = '.'
        if not os.path.isfile('Pipeline.db'):
            createTransferDB(dbFileName, dbDirectory, dbKey)
        pipelineRunDate = datetime.datetime.now()

        xferDB = TransferDB(db_file_name= dbFileName,
                            db_directory= dbDirectory,
                            db_key= dbKey,
                            pl_run_date= pipelineRunDate)
        conn = xferDB.connect_db()
        settingsDHIS = xferDB.config_dhis(conn, 'InSilicoVA')

        cls.pipelineDHIS = dhis.DHIS(settingsDHIS, '.')
        apiDHIS = cls.pipelineDHIS.connect()
        cls.postLog = cls.pipelineDHIS.post_va(apiDHIS)
        cls.pipelineDHIS.verify_post(cls.postLog, apiDHIS)

    def test_vaProgramUID(self):
        """Verify VA program is installed."""

        self.assertEqual(self.pipelineDHIS.va_program_uid, 'sv91bCroFFx')

    def test_postVA(self):
        """Post VA records to DHIS2."""

        checkLog = 'importSummaries' in self.postLog['response'].keys()
        self.assertTrue(checkLog)

    def test_verifyPost(self):
        """Verify VA records got posted to DHIS2."""

        dfNewStorage = read_csv('OpenVAFiles/newStorage.csv')
        nPushed = sum(dfNewStorage['pipelineOutcome'] == 'Pushed to DHIS2')
        self.assertEqual(nPushed, self.pipelineDHIS.n_posted_records)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        os.remove('OpenVAFiles/entityAttributeValue.csv')
        os.remove('OpenVAFiles/newStorage.csv')
        os.remove('Pipeline.db')


class Check_DHIS_getCODCode(unittest.TestCase):
    """Check getCODCode function."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        if os.path.isfile('who_cod.R'):
            os.remove('who_cod.R')
        # if os.path.isfile('tariff_cod.py'):
        #     os.remove('tariff_cod.py')

        pipelineRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(db_file_name='Pipeline.db',
                            db_directory='.',
                            db_key='enilepiP',
                            pl_run_date= pipelineRunDate)
        conn = xferDB.connect_db()
        cls.cod_who = xferDB.config_dhis(conn, 'InSilicoVA')
        cls.cod_tariff = xferDB.config_dhis(conn, 'SmartVA')

        with open("who_cod.R", "w", newline="") as f:
            f.write("data(causetextV5, package='InterVA5')\n")
            f.write("write.csv(causetextV5, file='who_cod.csv', row.names=FALSE)\n")
        rArgs = ["R", "CMD", "BATCH", "--vanilla", "who_cod.R"]
        subprocess.run(args=rArgs,
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
        # with open("tariff_cod.py", "w", newline="") as f:
        #     #print(r.text, file=f)
        #     f.write("ADULT = 'adult'\n")
        #     f.write("CHILD = 'child'\n")
        #     f.write("NEONATE = 'neonate'\n")
        #     print(tariff, file=f)
        # from tariff_cod import ICDS
        # tariff_adult = list(ICDS['adult'].keys())
        # tariff_child = list(ICDS['child'].keys())
        # tariff_neonate = list(ICDS['neonate'].keys())
        # cls.tariff_causes = tariff_adult + tariff_child + tariff_neonate

    def test_who_causes(self):
        """Test matching for WHO causes of death."""

        for i in self.who_causes:
            with self.subTest(i=i):
                self.assertIsNotNone(dhis.getCODCode(self.cod_who[1], i))

    # def test_tariff_causes(self):
    #     """Test matching for Tariff causes of death."""

    #     for i in self.tariff_causes:
    #         with self.subTest(i=i):
    #             self.assertIsNotNone(dhis.getCODCode(self.cod_tariff[1], i))

    @classmethod
    def tearDownClass(cls):

        os.remove('who_cod.csv')
        os.remove('who_cod.R')
        os.remove('who_cod.Rout')
        # os.remove('tariff_cod.py')


class Check_DHIS_Exceptions(unittest.TestCase):
    """Check that DHIS raises exceptions when it should."""


    def test_orgUnit_Exception(self):
        """Verify exception is raised with faulty input."""

        dirOpenVA = 'OpenVAFiles'
        dhisURL = 'https://va30se.swisstph-mis.ch'
        # dhisURL = 'https://va25.swisstph-mis.ch'
        dhisUser = 'va-demo'
        dhisPassword = 'VerbalAutopsy99!'
        dhisOrgUnit = 'wrong'
        ntDHIS = collections.namedtuple('ntDHIS',
                                        ['dhisURL',
                                         'dhisUser',
                                         'dhisPassword',
                                         'dhisOrgUnit',
                                         'dhisCODCodes']
        )
        badSettings = ntDHIS(dhisURL,
                             dhisUser,
                             dhisPassword,
                             dhisOrgUnit,
                             'InSilicoVA')
        mockCOD = {'cause1': 'code1', 'cause2': 'code2'}
        badInput = [badSettings, mockCOD]
        pipelineDHIS = dhis.DHIS(badInput, '.')
        self.assertRaises(dhis.DHISError, pipelineDHIS.connect)

    def test_dhisUser_Exception(self):
        """Verify exepction is raised with faulty input."""

        dirOpenVA = 'OpenVAFiles'
        dhisURL = 'https://va30se.swisstph-mis.ch'
        # dhisURL = 'https://va25.swisstph-mis.ch'
        dhisUser = 'wrong'
        dhisPassword = 'VerbalAutopsy99!'
        dhisOrgUnit = 'SCVeBskgiK6'
        ntDHIS = collections.namedtuple('ntDHIS',
                                        ['dhisURL',
                                         'dhisUser',
                                         'dhisPassword',
                                         'dhisOrgUnit',
                                         'dhisCODCodes']
        )
        badSettings = ntDHIS(dhisURL,
                             dhisUser,
                             dhisPassword,
                             dhisOrgUnit,
                             'InSilicoVA')
        mockCOD = {'cause1': 'code1', 'cause2': 'code2'}
        badInput = [badSettings, mockCOD]

        pipelineDHIS = dhis.DHIS(badInput, '.')
        self.assertRaises(dhis.DHISError, pipelineDHIS.connect)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('DHIS/blobs/', ignore_errors = True)


if __name__ == '__main__':
    unittest.main(verbosity = 2)
