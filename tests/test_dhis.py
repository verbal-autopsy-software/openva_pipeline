#------------------------------------------------------------------------------#
# test_dhis.py
#------------------------------------------------------------------------------#

import datetime
import subprocess
import shutil
import os
import unittest
import collections
import pandas as pd
from pysqlcipher3 import dbapi2 as sqlcipher

import context
from openva_pipeline import dhis
from openva_pipeline.transferDB import TransferDB

os.chdir(os.path.abspath(os.path.dirname(__file__)))

class Check_DHIS(unittest.TestCase):
    """Check the everything works as it should."""

    shutil.rmtree("DHIS2/blobs/", ignore_errors = True)
    shutil.copy("OpenVAFiles/sampleEAV.csv",
                "OpenVAFiles/entityAttributeValue.csv")
    shutil.copy("OpenVAFiles/sample_recordStorage.csv",
                "OpenVAFiles/recordStorage.csv")
    # Define valid parameters for SwissTPH DHIS2 Server.
    dirOpenVA = "OpenVAFiles"
    dhisURL = "https://va30se.swisstph-mis.ch"
    # dhisURL = "https://va25.swisstph-mis.ch"
    dhisUser = "va-demo"
    dhisPassword = "VerbalAutopsy99!"
    dhisOrgUnit = "SCVeBskgiK6"

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
    settingsDHIS = xferDB.configDHIS(conn, "InSilicoVA")

    pipelineDHIS = dhis.DHIS(settingsDHIS, ".")
    apiDHIS = pipelineDHIS.connect()
    postLog = pipelineDHIS.postVA(apiDHIS)

    def test_DHIS_1_vaProgramUID(self):
        """Verify VA program is installed."""
        self.assertEqual(self.pipelineDHIS.vaProgramUID, "sv91bCroFFx")

    def test_DHIS_2_postVA(self):
        """Post VA records to DHIS2."""
        checkLog = 'importSummaries' in self.postLog['response'].keys()
        self.assertTrue(checkLog)

    def test_DHIS_3_verifyPost(self):
        """Verify VA records got posted to DHIS2."""
        self.pipelineDHIS.verifyPost(self.postLog, self.apiDHIS)
        dfNewStorage = pd.read_csv("OpenVAFiles/newStorage.csv")
        nPushed = sum(dfNewStorage['pipelineOutcome'] == "Pushed to DHIS2")
        self.assertEqual(nPushed, self.pipelineDHIS.nPostedRecords)

class Check_DHIS_Exceptions(unittest.TestCase):
    """Check that DHIS raises exceptions when it should."""

    def test_DHIS_orgUnit_Exception(self):
        """Verify exception is raised with faulty input."""
        dirOpenVA = "OpenVAFiles"
        dhisURL = "https://va30se.swisstph-mis.ch"
        dhisUser = "va-demo"
        dhisPassword = "VerbalAutopsy99!"
        dhisOrgUnit = "wrong"
        ntDHIS = collections.namedtuple("ntDHIS",
                                        ["dhisURL",
                                         "dhisUser",
                                         "dhisPassword",
                                         "dhisOrgUnit",
                                         "dhisCODCodes"]
        )
        badSettings = ntDHIS(dhisURL,
                             dhisUser,
                             dhisPassword,
                             dhisOrgUnit,
                             "InSilicoVA")
        mockCOD = {"cause1": "code1", "cause2": "code2"}
        badInput = [badSettings, mockCOD]
        pipelineDHIS = dhis.DHIS(badInput, ".")
        self.assertRaises(dhis.DHISError, pipelineDHIS.connect)

    def test_DHIS_dhisUser_Exception(self):
        """Verify exepction is raised with faulty input."""
        dirOpenVA = "OpenVAFiles"
        dhisURL = "https://va30se.swisstph-mis.ch"
        dhisUser = "wrong"
        dhisPassword = "VerbalAutopsy99!"
        dhisOrgUnit = "SCVeBskgiK6"
        ntDHIS = collections.namedtuple("ntDHIS",
                                        ["dhisURL",
                                         "dhisUser",
                                         "dhisPassword",
                                         "dhisOrgUnit",
                                         "dhisCODCodes"]
        )
        badSettings = ntDHIS(dhisURL,
                             dhisUser,
                             dhisPassword,
                             dhisOrgUnit,
                             "InSilicoVA")
        mockCOD = {"cause1": "code1", "cause2": "code2"}
        badInput = [badSettings, mockCOD]

        pipelineDHIS = dhis.DHIS(badInput, ".")
        self.assertRaises(dhis.DHISError, pipelineDHIS.connect)

if __name__ == "__main__":
    unittest.main()
