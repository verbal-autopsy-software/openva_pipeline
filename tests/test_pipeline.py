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
from openva_pipeline.dhis import DHIS
from openva_pipeline.dhis import API
from openva_pipeline.dhis import VerbalAutopsyEvent
from openva_pipeline.odk import ODK
from openva_pipeline.transferDB import TransferDB
from openva_pipeline.pipeline import Pipeline
from openva_pipeline.runPipeline import downloadBriefcase
from openva_pipeline.runPipeline import downloadSmartVA
from openva_pipeline.runPipeline import createTransferDB

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class Check_Pipeline_config(unittest.TestCase):
    """Check config method:"""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        pl = Pipeline('Pipeline.db', '.', 'enilepiP', True)
        settings = pl.config()
        cls.settingsPipeline = settings['pipeline']
        cls.settingsODK = settings['odk']
        cls.settingsOpenVA = settings['openVA']
        cls.settingsDHIS = settings['dhis']

    def test_config_pipeline_algorithmMetadataCode(self):
        """Test config method configuration of pipeline:
        settingsPipeline.algorithmMetadataCode:"""

        self.assertEqual(self.settingsPipeline.algorithmMetadataCode,
            'InterVA5|5|InterVA|5|2016 WHO Verbal Autopsy Form|v1_5_1')

    def test_config_pipeline_codSource(self):
        """Test config method configuration of pipeline:
        settingsPipeline.codSource:"""

        self.assertEqual(self.settingsPipeline.codSource, 'WHO')

    def test_config_pipeline_algorithm(self):
        """Test config method configuration of pipeline:
        settingsPipeline.algorithm:"""

        self.assertEqual(self.settingsPipeline.algorithm, 'InterVA')

    def test_config_pipeline_workingDirecotry(self):
        """Test config method configuration of pipeline:
        settingsPipeline.workingDirectory:"""

        self.assertEqual(self.settingsPipeline.workingDirectory, '.')

    def test_config_odk_odkID(self):
        """Test config method configuration of pipeline:
        settingsODK.odkID:"""

        self.assertEqual(self.settingsODK.odkID, None)

    def test_config_odk_odkURL(self):
        """Test config method configuration of pipeline:
        settingsODK.odkURL:"""

        self.assertEqual(self.settingsODK.odkURL,
                         'https://odk.swisstph.ch/ODKAggregateOpenVa')

    def test_config_odk_odkUser(self):
        """Test config method configuration of pipeline:
        settingsODK.odkUser:"""

        self.assertEqual(self.settingsODK.odkUser, 'odk_openva')

    def test_config_odk_odkPassword(self):
        """Test config method configuration of pipeline:
        settingsODK.odkPassword:"""

        self.assertEqual(self.settingsODK.odkPassword, 'openVA2018')

    def test_config_odk_odkFormID(self):
        """Test config method configuration of pipeline:
        settingsODK.odkFormID:"""

        self.assertEqual(self.settingsODK.odkFormID,
                         'va_who_v1_5_1')

    def test_config_odk_odkLastRun(self):
        """Test config method configuration of pipeline:
        settingsODK.odkLastRun:"""

        self.assertEqual(self.settingsODK.odkLastRun, '1900-01-01_00:00:01')

    def test_config_odk_odkUseCentral(self):
        """Test config method configuration of pipeline:
        settingsODK.odkUseCentral:"""

        self.assertEqual(self.settingsODK.odkUseCentral, 'False')

    def test_config_odk_odkProjectNumber(self):
        """Test config method configuration of pipeline:
        settingsODK.odkProjectNumber:"""

        self.assertEqual(self.settingsODK.odkProjectNumber, '40')

    def test_config_dhis_dhisURL(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisURL:"""

        self.assertEqual(self.settingsDHIS[0].dhisURL, 'https://va30se.swisstph-mis.ch')

    def test_config_dhis_dhisUser(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisUser:"""

        self.assertEqual(self.settingsDHIS[0].dhisUser, 'va-demo')

    def test_config_dhis_dhisPassword(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisPassword:"""

        self.assertEqual(self.settingsDHIS[0].dhisPassword, 'VerbalAutopsy99!')

    def test_config_dhis_dhisOrgUnit(self):
        """Test config method configuration of pipeline:
        settingsDHIS.dhisOrgUnit:"""

        self.assertEqual(self.settingsDHIS[0].dhisOrgUnit, 'SCVeBskgiK6')

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class DownloadAppsTests(unittest.TestCase):
    """Check the methods for downloading external apps:"""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        cls.pl = Pipeline('Pipeline.db', '.', 'enilepiP', True)

    def test_downloadBriefcase(self):
        """Check downloadBriefcase():"""

        if os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            os.remove('ODK-Briefcase-v1.18.0.jar')
        downloadBriefcase()
        self.assertTrue(os.path.isfile('ODK-Briefcase-v1.18.0.jar'))

    def test_downloadSmartVA(self):
        """Check downloadSmartVA():"""

        if os.path.isfile('smartva'):
            os.remove('smartva')
        downloadSmartVA()
        self.assertTrue(os.path.isfile('smartva'))

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_runODK_clean(unittest.TestCase):
    """Check runODK method on initial run:"""


    @classmethod
    def setUpClass(cls):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors = True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if not os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            downloadBriefcase()
        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

        pl = Pipeline('Pipeline.db', '.', 'enilepiP', True)
        settings = pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']
        cls.odkBC = pl.runODK(settingsODK, settingsPipeline)

    def test_clean_runODK_returncode(self):
        """Check returncode with valid parameters:"""

        self.assertEqual(0, self.odkBC.returncode)

    def test_clean_runODK_creates_odkBCExportNew(self):
        """Check for exported CSV file:"""

        self.assertTrue(os.path.isfile('ODKFiles/odkBCExportNew.csv'))

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors = True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        os.remove('Pipeline.db')


class Check_runODK_with_exports(unittest.TestCase):
    """Check runODK method with existing ODK exports:"""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

    def setUp(self):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors = True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv', 'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv', 'ODKFiles/odkBCExportNew.csv')
        self.old_mtimePrev = os.path.getmtime('ODKFiles/odkBCExportPrev.csv')
        self.old_mtimeNew = os.path.getmtime('ODKFiles/odkBCExportNew.csv')
        if not os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            downloadBriefcase()
        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        self.dbFileName = 'Pipeline.db'
        self.dbDirectory = '.'
        self.dbKey = 'enilepiP'
        self.useDHIS = True
        self.pl = Pipeline(self.dbFileName, self.dbDirectory,
                           self.dbKey, self.useDHIS)
        settings = self.pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']
        self.odkBC = self.pl.runODK(settingsODK, settingsPipeline)
        self.new_mtimePrev = os.path.getmtime('ODKFiles/odkBCExportPrev.csv')
        self.new_mtimeNew = os.path.getmtime('ODKFiles/odkBCExportNew.csv')

    def tearDown(self):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors = True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')

    def test_runODK_returncode_with_previous_exports(self):
        """Check returncode with valid parameters:"""

        self.assertEqual(0, self.odkBC.returncode)

    def test_runODK_exportPrev_with_previous_exports(self):
        """Check modification time on odkBCExportPrev with previous exports:"""

        self.assertTrue(self.new_mtimePrev > self.old_mtimePrev)

    def test_runODK_exportNew_with_previous_exports(self):
        """Check modification time on odkBCExportNew:"""

        self.assertTrue(self.new_mtimeNew > self.old_mtimeNew)

    def test_runODK_mergeToPrevExport_with_previous_exports(self):
        """Check mergeToPrevExport() keeps all records from BC export files:"""

        hasAll = True
        with open('ODKFiles/odkBCExportPrev.csv') as fCombined:
            fCombinedLines = fCombined.readlines()
        with open('ODKFiles/previous_bc_export.csv') as fPrevious:
            fPreviousLines = fPrevious.readlines()
        with open('ODKFiles/another_bc_export.csv') as fAnother:
            fAnotherLines = fAnother.readlines()
        for line in fPreviousLines:
            if line not in fCombinedLines:
                hasAll = False
        for line in fAnotherLines:
            if line not in fCombinedLines:
                hasAll = False
        self.assertTrue(hasAll)

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_storeResultsDB(unittest.TestCase):
    """Check storeResultsDB method marks duplicate records:"""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

    def setUp(self):

        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors = True)
        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if not os.path.isfile('ODK-Briefcase-v1.18.0.jar'):
            downloadBriefcase()
        self.pl = Pipeline('Pipeline.db', '.', 'enilepiP', True)
        self.settings = self.pl.config()
        self.settingsPipeline = self.settings['pipeline']
        self.settingsODK = self.settings['odk']
        self.settingsOpenVA = self.settings['openVA']
        self.settingsDHIS = self.settings['dhis']

        self.xferDB = TransferDB(dbFileName = 'Pipeline.db', dbDirectory = '.',
                                 dbKey = 'enilepiP', plRunDate = True)
        self.conn = self.xferDB.connectDB()
        self.c = self.conn.cursor()
        self.c.execute('DELETE FROM EventLog;')
        self.conn.commit()
        self.c.execute('DELETE FROM VA_Storage;')
        self.conn.commit()
        self.odkBC = self.pl.runODK(self.settingsODK, self.settingsPipeline)

    def test_runODK_checkDuplicates(self):
        """Check checkDuplicates() method:"""

        vaRecords = read_csv('ODKFiles/odkBCExportNew.csv')
        nVA = vaRecords.shape[0]
        rOut = self.pl.runOpenVA(self.settingsOpenVA,
                                 self.settingsPipeline,
                                 self.settingsODK.odkID,
                                 self.pl.pipelineRunDate)
        pipelineDHIS = self.pl.runDHIS(self.settingsDHIS,
                                       self.settingsPipeline)
        self.pl.storeResultsDB()
        os.remove('ODKFiles/odkBCExportNew.csv')
        os.remove('OpenVAFiles/pycrossva_input.csv')
        os.remove('OpenVAFiles/openVA_input.csv')
        odkBC2 = self.pl.runODK(self.settingsODK,
                                self.settingsPipeline)
        self.c.execute('SELECT eventDesc FROM EventLog;')
        query = self.c.fetchall()
        nDuplicates = [i[0] for i in query if 'duplicate' in i[0]]
        self.assertEqual(len(nDuplicates), nVA)

    def tearDown(self):

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        shutil.rmtree('ODKFiles/ODK Briefcase Storage/', ignore_errors = True)
        self.conn.close()

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')


class Check_runOpenVA(unittest.TestCase):
    """Check runOpenVA method sets up files correctly"""


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/odkExport_prev_who_v151.csv',
            'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/odkExport_new_who_v151.csv',
            'ODKFiles/odkBCExportNew.csv')
        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

        pl = Pipeline('Pipeline.db', '.', 'enilepiP', True)
        settings = pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']
        cls.rOut = pl.runOpenVA(settingsOpenVA, settingsPipeline,
                                settingsODK.odkID, pl.pipelineRunDate)

    def test_creates_openVA_input_csv(self):
        """Check that runOpenVA() brings in new file:"""

        self.assertTrue(
            os.path.isfile('OpenVAFiles/openVA_input.csv')
        )

    def test_merges_records(self):
        """Check that runOpenVA() includes all records:"""

        hasAll = True
        # with open('OpenVAFiles/openVA_input.csv') as fCombined:
        with open('OpenVAFiles/pycrossva_input.csv') as fCombined:
            fCombinedLines = fCombined.readlines()
        with open('ODKFiles/odkExport_prev_who_v151.csv') as fPrevious:
            fPreviousLines = fPrevious.readlines()
        with open('ODKFiles/odkExport_new_who_v151.csv') as fAnother:
            fAnotherLines = fAnother.readlines()
        for line in fPreviousLines:
            if line not in fCombinedLines:
                hasAll = False
        for line in fAnotherLines:
            if line not in fCombinedLines:
                hasAll = False
        self.assertTrue(hasAll)

    def test_zeroRecords_false(self):
        """Check that runOpenVA() returns zeroRecords = FALSE"""

        self.assertFalse(self.rOut['zeroRecords'])

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        os.remove('Pipeline.db')


class Check_runOpenVA_zeroRecords(unittest.TestCase):
    """Check runOpenVA method sets up files correctly"""


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/zeroRecords_bc_export.csv',
                    'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/zeroRecords_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')

        plZero = Pipeline('copy_Pipeline.db', '.', 'enilepiP', True)
        settings = plZero.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']
        cls.rOut = plZero.runOpenVA(settingsOpenVA,
                                     settingsPipeline,
                                     settingsODK.odkID,
                                     plZero.pipelineRunDate)

    def test_zeroRecords_true(self):
        """Check that runOpenVA() returns zeroRecords == True:"""

        self.assertTrue(self.rOut['zeroRecords'])

    def test_zeroRecords_no_input_csv(self):
        """Check that runOpenVA() doesn't create new file if zeroRecords:"""

        self.assertFalse(
            os.path.isfile('OpenVAFiles/openVA_input.csv')
        )

    @classmethod
    def tearDownClass(cls):

        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')


class Check_Pipeline_runOpenVA_InSilicoVA(unittest.TestCase):
    """Check runOpenVA method runs InSilicoVA"""


    @classmethod
    def setUpClass(cls):

        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime('%Y-%m-%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName = 'copy_Pipeline.db', dbDirectory = '.',
                            dbKey = 'enilepiP', plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()

        c = conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InSilicoVA', 'InSilicoVA|1.1.4|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1');
        c.execute(sql, par)
        sql = 'UPDATE InSilicoVA_Conf SET data_type = ?'
        par = ('WHO2016',);
        c.execute(sql, par)
        conn.commit()
        conn.close()
        cls.pl = Pipeline('copy_Pipeline.db', '.', 'enilepiP', True)
        settings = cls.pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')

        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')

        cls.rOut = cls.pl.runOpenVA(settingsOpenVA, settingsPipeline,
                                    settingsODK.odkID, cls.pl.pipelineRunDate)

    def test_runOpenVA_InSilico_R(self):
        """Check that runOpenVA() creates an R script for InSilicoVA:"""

        rScriptFile = os.path.join('OpenVAFiles',
                                   self.pl.pipelineRunDate,
                                   'Rscript_' + self.pl.pipelineRunDate + '.R')
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InSilico_Rout(self):
        """Check that runOpenVA() runs R script for InSilicoVA:"""

        rScriptFile = os.path.join('OpenVAFiles',
                                   self.pl.pipelineRunDate,
                                   'Rscript_' + self.pl.pipelineRunDate + '.Rout')
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InSilico_completed(self):
        """Check that runOpenVA() creates an R script for InSilicoVA:"""

        self.assertEqual(self.rOut['returncode'], 0)

    @classmethod
    def tearDownClass(cls):

        # shutil.rmtree('OpenVAFiles/' + cls.pl.pipelineRunDate,
        #               ignore_errors = True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')


class Check_Pipeline_runOpenVA_InterVA(unittest.TestCase):
    """Check runOpenVA method runs InterVA"""


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('InterVA_Pipeline.db'):
            os.remove('InterVA_Pipeline.db')
        createTransferDB('InterVA_Pipeline.db', '.', 'enilepiP')
        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime('%Y-%m-%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName = 'InterVA_Pipeline.db', dbDirectory = '.',
                            dbKey = 'enilepiP', plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InterVA', 'InterVA5|5|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        conn.commit()
        conn.close()
        cls.pl = Pipeline('copy_Pipeline.db', '.', 'enilepiP', True)
        settings = cls.pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')

        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')

        cls.rOut = cls.pl.runOpenVA(settingsOpenVA, settingsPipeline,
                                    settingsODK.odkID, cls.pl.pipelineRunDate)

    def test_runOpenVA_InterVA_R(self):
        """Check that runOpenVA() creates an R script for InterVA:"""

        rScriptFile = os.path.join('OpenVAFiles',
                                   self.pl.pipelineRunDate,
                                   'Rscript_' + self.pl.pipelineRunDate + '.R')
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InterVA_Rout(self):
        """Check that runOpenVA() runs R script for InterVA:"""

        rScriptFile = os.path.join('OpenVAFiles',
                                   self.pl.pipelineRunDate,
                                   'Rscript_' + self.pl.pipelineRunDate + '.Rout')
        self.assertTrue(os.path.isfile(rScriptFile))

    def test_runOpenVA_InterVA_completed(self):
        """Check that runOpenVA() creates an R script for InterVA:"""

        self.assertEqual(self.rOut['returncode'], 0)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('OpenVAFiles/' + cls.pl.pipelineRunDate,
                      ignore_errors = True)
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')
        if os.path.isfile('InterVA_Pipeline.db'):
            os.remove('InterVA_Pipeline.db')


class Check_runOpenVA_SmartVA(unittest.TestCase):
    """Check runOpenVA method runs SmartVA"""


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-1.csv',
                    'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/odkExport_phmrc-2.csv',
                    'ODKFiles/odkBCExportNew.csv')
        if not os.path.isfile('smartva'):
            downloadSmartVA()

        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime('%Y-%m-%d_%H:%M:%S')
        cls.pl = Pipeline('copy_smartVA_Pipeline.db', '.', 'enilepiP', True)
        settings = cls.pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']

        cls.rOut = cls.pl.runOpenVA(settingsOpenVA, settingsPipeline,
                                    settingsODK.odkID, cls.pl.pipelineRunDate)
        cls.svaOut = os.path.join(
            'OpenVAFiles',
            cls.pl.pipelineRunDate,
            '1-individual-cause-of-death/individual-cause-of-death.csv'
        )

    def test_runOpenVA_SmartVA_results(self):
        """Check that runOpenVA() executes SmartVA cli"""

        self.assertTrue(os.path.isfile(self.svaOut))

    def test_runOpenVA_SmartVA_eva(self):
        """Check that runOpenVA() creates EAV output for SmartVA """

        self.assertTrue(os.path.isfile('OpenVAFiles/entityAttributeValue.csv'))

    def test_runOpenVA_SmartVA_recordStorage(self):
        """Check that runOpenVA() creates recordStorage.csv for SmartVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/recordStorage.csv'))

    def test_runOpenVA_SmartVA_completed(self):
        """Check runOpenVA() returncoe for SmartVA:"""

        self.assertEqual(self.rOut['returncode'], 0)

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.pl.pipelineRunDate),
            ignore_errors = True
        )
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')
 

class Check_Pipeline_runDHIS(unittest.TestCase):
    """Check runDHIS method"""


    @classmethod
    def setUpClass(cls):

        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')
        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        shutil.copy('OpenVAFiles/sampleEAV.csv',
                    'OpenVAFiles/entityAttributeValue.csv')
        shutil.copy('OpenVAFiles/sample_recordStorage.csv',
                    'OpenVAFiles/recordStorage.csv')
        shutil.copy('OpenVAFiles/sample_newStorage.csv',
                    'OpenVAFiles/newStorage.csv')
        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

        pl = Pipeline('Pipeline.db', '.', 'enilepiP', True)
        plRunDate = pl.pipelineRunDate
        settings = pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']
        cls.pipelineDHIS = pl.runDHIS(settingsDHIS, settingsPipeline)

    def test_runDHIS_vaProgramUID(self):
        """Verify VA program is installed:"""

        self.assertEqual(self.pipelineDHIS['vaProgramUID'], 'sv91bCroFFx')

    def test_runDHIS_postVA(self):
        """Post VA records to DHIS2:"""

        postLog = self.pipelineDHIS['postLog']
        checkLog = 'importSummaries' in postLog['response'].keys()
        self.assertTrue(checkLog)

    def test_runDHIS_verifyPost(self):
        """Verify VA records got posted to DHIS2:"""

        dfNewStorage = read_csv('OpenVAFiles/newStorage.csv')
        nPushed = sum(dfNewStorage['pipelineOutcome'] == 'Pushed to DHIS2')
        self.assertEqual(nPushed, self.pipelineDHIS['nPostedRecords'])

    @classmethod
    def tearDownClass(cls):

        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')
        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')
        if os.path.isfile('OpenVAFiles/newStorage.csv'):
            os.remove('OpenVAFiles/newStorage.csv')
        os.remove('Pipeline.db')


class Check_Pipeline_depositResults(unittest.TestCase):
    """Store VA results in Transfer database."""


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')
        if os.path.isfile('OpenVAFiles/newStorage.csv'):
            os.remove('OpenVAFiles/newStorage.csv')
        shutil.copy('OpenVAFiles/sample_newStorage.csv',
                    'OpenVAFiles/newStorage.csv')
        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime('%Y-%m-%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName = 'Pipeline.db',
                            dbDirectory = '.',
                            dbKey = 'enilepiP',
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        c.execute('DELETE FROM VA_Storage;')
        conn.commit()
        conn.close()
        pl = Pipeline('Pipeline.db', '.', 'enilepiP', True)
        settings = pl.config()
        settingsPipeline = settings['pipeline']
        settingsODK = settings['odk']
        settingsOpenVA = settings['openVA']
        settingsDHIS = settings['dhis']

        pl.storeResultsDB()

        xferDB = TransferDB(dbFileName = 'Pipeline.db',
                            dbDirectory = '.',
                            dbKey = 'enilepiP',
                            plRunDate = pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sql = 'SELECT id FROM VA_Storage'
        c.execute(sql)
        vaIDs = c.fetchall()
        conn.close()
        vaIDsList = [j for i in vaIDs for j in i]
        cls.s1 = set(vaIDsList)
        dfNewStorage = read_csv('OpenVAFiles/newStorage.csv')
        dfNewStorageID = dfNewStorage['odkMetaInstanceID']
        cls.s2 = set(dfNewStorageID)

    def test_storeVA(self):
        """Check that depositResults() stores VA records in Transfer DB:"""

        self.assertTrue(self.s2.issubset(self.s1))

    @classmethod
    def tearDownClass(cls):
        if os.path.isfile('OpenVAFiles/newStorage.csv'):
            os.remove('OpenVAFiles/newStorage.csv')
        os.remove('Pipeline.db')


class Check_Pipeline_cleanPipeline(unittest.TestCase):
    'Update ODK_Conf ODKLastRun in Transfer DB and clean up files.'


    @classmethod
    def setUpClass(cls):

        if not os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            shutil.copy('ODKFiles/previous_bc_export.csv',
                        'ODKFiles/odkBCExportPrev.csv')
        if not os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            shutil.copy('ODKFiles/another_bc_export.csv',
                        'ODKFiles/odkBCExportNew.csv')

        if not os.path.isfile('OpenVAFiles/openVA_input.csv'):
            shutil.copy('OpenVAFiles/sample_openVA_input.csv',
                        'OpenVAFiles/openVA_input.csv')
        if not os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            shutil.copy('OpenVAFiles/sampleEAV.csv',
                        'OpenVAFiles/entityAttributeValue.csv')
        if not os.path.isfile('OpenVAFiles/recordStorage.csv'):
            shutil.copy('OpenVAFiles/sample_recordStorage.csv',
                        'OpenVAFiles/recordStorage.csv')
        if not os.path.isfile('OpenVAFiles/newStorage.csv'):
            shutil.copy('OpenVAFiles/sample_newStorage.csv',
                        'OpenVAFiles/newStorage.csv')

        os.makedirs('DHIS/blobs/', exist_ok = True)
        shutil.copy('OpenVAFiles/sample_newStorage.csv',
                    'DHIS/blobs/001-002-003.db')

        nowDate = datetime.datetime.now()
        pipelineRunDate = nowDate.strftime('%Y-%m-%d_%H:%M:%S')
        cls.pl = Pipeline('copy_Pipeline.db', '.', 'enilepiP', True)
        cls.pl.closePipeline()

        xferDB = TransferDB(dbFileName = 'copy_Pipeline.db',
                            dbDirectory = '.',
                            dbKey = 'enilepiP',
                            plRunDate = pipelineRunDate)
        cls.conn = xferDB.connectDB()
        cls.c = cls.conn.cursor()

    def test_cleanPipeline_rmFiles(self):
        """Test file removal:"""

        fileExist = False
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            fileExist = True
            print('Problem: found ODKFiles/odkBCExportNew.csv \n')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            fileExist = True
            print('Problem: found ODKFiles/odkBCExportPrev.csv \n')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            fileExist = True
            print('Problem: found OpenVAFiles/openVA_input.csv \n')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            fileExist = True
            print('Problem: found OpenVAFiles/entityAttributeValue.csv \n')
        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            fileExist = True
            print('Problem: found OpenVAFiles/recordStorage.csv \n')
        if os.path.isfile('OpenVAFiles/newStorage.csv'):
            fileExist = True
        if os.path.isfile('DHIS/blobs/001-002-003.db'):
            fileExist = True
        self.assertFalse(fileExist)

    def test_cleanPipeline_odkLastRun(self):
        """Test update of ODK_Conf.odkLastRun:"""

        self.c.execute('SELECT odkLastRun FROM ODK_Conf;')
        sqlQuery = self.c.fetchone()
        results = [i for i in sqlQuery]
        self.assertEqual(results[0], self.pl.pipelineRunDate)

    @classmethod
    def tearDownClass(cls):

        cls.c.execute("UPDATE ODK_Conf SET odkLastRun = '1900-01-01_00:00:01';")
        cls.conn.commit()
        cls.conn.close()
        shutil.rmtree('DHIS/blobs/', ignore_errors = True)
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')
        if os.path.isfile('OpenVAFiles/recordStorage.csv'):
            os.remove('OpenVAFiles/recordStorage.csv')
        if os.path.isfile('OpenVAFiles/entityAttributeValue.csv'):
            os.remove('OpenVAFiles/entityAttributeValue.csv')
        if os.path.isfile('OpenVAFiles/newStorage.csv'):
            os.remove('OpenVAFiles/newStorage.csv')


if __name__ == '__main__':
    unittest.main(verbosity = 2)
