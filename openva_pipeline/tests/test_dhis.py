#------------------------------------------------------------------------------#
# test_dhis.py
#------------------------------------------------------------------------------#

import datetime
import subprocess
import shutil
import os
import unittest
import collections
from context import dhis
from dhis import DHIS
from dhis import API
from dhis import VerbalAutopsyEvent
from context import transferDB
from transferDB import TransferDB
from pysqlcipher3 import dbapi2 as sqlcipher

os.chdir(os.path.abspath(os.path.dirname(__file__)))

class Check_DHIS_Connect(unittest.TestCase):
    """Check the everything works as it should."""

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

    xferDB = TransferDB(dbFileName = dbFileName,
                        dbDirectory = dbDirectory,
                        dbKey = dbKey)
    conn = xferDB.connectDB()
    settingsDHIS = xferDB.configDHIS(conn, "InSilicoVA")

    # ntDHIS = collections.namedtuple("ntDHIS",
    #                                 ["dhisURL",
    #                                  "dhisUser",
    #                                  "dhisPassword",
    #                                  "dhisOrgUnit",
    #                                  "dhisCODCodes"]
    # )
    # settingsDHIS = ntDHIS(dhisURL,
    #                       dhisUser,
    #                       dhisPassword,
    #                       dhisOrgUnit,
    #                       "InSilcoVA")
    # ntDHIS = collections.namedtuple("ntDHIS",
    #                                 ["dhisURL",
    #                                  "dhisUser",
    #                                  "dhisPassword",
    #                                  "dhisOrgUnit"]
    # )
    # settingsDHIS = [ntDHIS(dhisURL,
    #                        dhisUser,
    #                        dhisPassword,
    #                        dhisOrgUnit),
    #                 "InSilcoVA"]

    def test_DHIS_vaProgramUID(self):
        """Verify VA program is installed."""
        pipelineDHIS = dhis.DHIS(self.settingsDHIS, ".")
        pipelineDHIS.connect()
        self.assertEqual(pipelineDHIS.vaProgramUID, "sv91bCroFFx")

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
        badInput = ntDHIS(dhisURL,
                          dhisUser,
                          dhisPassword,
                          dhisOrgUnit,
                          "InSilicoVA")
        badInput = [badInput, self.settingsDHIS[1]]
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
        badInput = ntDHIS(dhisURL,
                          dhisUser,
                          dhisPassword,
                          dhisOrgUnit,
                          "InSilicoVA")
        badInput = [badInput, self.settingsDHIS[1]]
        pipelineDHIS = dhis.DHIS(badInput, ".")
        self.assertRaises(dhis.DHISError, pipelineDHIS.connect)

    def test_DHIS_postVA(self):
        """Post VA records to DHIS."""

        # if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
        #     os.remove("OpenVAFiles/entityAttributeValue.csv")
        shutil.copy("OpenVAFiles/sampleEAV.csv",
                    "OpenVAFiles/entityAttributeValue.csv")

        pipelineDHIS = dhis.DHIS(self.settingsDHIS, ".")
        apiDHIS = pipelineDHIS.connect()
        postLog = pipelineDHIS.postVA(apiDHIS)
        checkLog = 'importSummaries' in postLog['response'].keys()
        self.assertTrue(checkLog)

if __name__ == "__main__":
    unittest.main()
