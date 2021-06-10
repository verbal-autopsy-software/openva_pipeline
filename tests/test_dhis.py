import datetime
import subprocess
import shutil
import os
import unittest
import collections
from pandas import read_csv
from pysqlcipher3 import dbapi2 as sqlcipher

from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context
from openva_pipeline import dhis
from openva_pipeline.transferDB import TransferDB
from openva_pipeline.runPipeline import createTransferDB

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class Check_DHIS(unittest.TestCase):
    """Check the everything works as it should."""


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
        # dbDirectory = os.path.abspath(os.path.dirname(__file__))
        dbDirectory = '.'
        if not os.path.isfile('Pipeline.db'):
            createTransferDB(dbFileName, dbDirectory, dbKey)
        pipelineRunDate = datetime.datetime.now()

        xferDB = TransferDB(dbFileName = dbFileName,
                            dbDirectory = dbDirectory,
                            dbKey = dbKey,
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        settingsDHIS = xferDB.configDHIS(conn, 'InSilicoVA')

        cls.pipelineDHIS = dhis.DHIS(settingsDHIS, '.')
        apiDHIS = cls.pipelineDHIS.connect()
        cls.postLog = cls.pipelineDHIS.postVA(apiDHIS)
        cls.pipelineDHIS.verifyPost(cls.postLog, apiDHIS)

    def test_vaProgramUID(self):
        """Verify VA program is installed."""

        self.assertEqual(self.pipelineDHIS.vaProgramUID, 'sv91bCroFFx')

    def test_postVA(self):
        """Post VA records to DHIS2."""

        checkLog = 'importSummaries' in self.postLog['response'].keys()
        self.assertTrue(checkLog)

    def test_verifyPost(self):
        """Verify VA records got posted to DHIS2."""

        dfNewStorage = read_csv('OpenVAFiles/newStorage.csv')
        nPushed = sum(dfNewStorage['pipelineOutcome'] == 'Pushed to DHIS2')
        self.assertEqual(nPushed, self.pipelineDHIS.nPostedRecords)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        os.remove('OpenVAFiles/entityAttributeValue.csv')
        os.remove('OpenVAFiles/newStorage.csv')
        os.remove('Pipeline.db')


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
