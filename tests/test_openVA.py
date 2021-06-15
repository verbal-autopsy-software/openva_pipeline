import unittest
import os
import shutil
import collections
from pysqlcipher3 import dbapi2 as sqlcipher
import datetime

from sys import path
source_path = os.path.dirname(os.path.abspath(__file__))
path.append(source_path)
import context
from openva_pipeline.transferDB import TransferDB
from openva_pipeline.openVA import OpenVA
from openva_pipeline.runPipeline import downloadSmartVA
from openva_pipeline.runPipeline import createTransferDB
from openva_pipeline.exceptions import OpenVAError
from openva_pipeline.exceptions import SmartVAError

os.chdir(os.path.abspath(os.path.dirname(__file__)))


class Check_copyVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')
        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

        # pipelineRunDate = datetime.datetime.now()
        pipelineRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='Pipeline.db',
                            dbDirectory='.',
                            dbKey='enilepiP',
                            plRunDate=pipelineRunDate)
        conn = xferDB.connectDB()
        settingsPipeline = xferDB.configPipeline(conn)
        settingsODK = xferDB.configODK(conn)
        settingsInterVA = xferDB.configOpenVA(conn,
                                              'InterVA',
                                              settingsPipeline.workingDirectory)
        cls.staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')

        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )

        rOpenVA = OpenVA(vaArgs=settingsInterVA,
                         pipelineArgs=settingsPipeline,
                         odkID=settingsODK.odkID,
                         runDate=cls.staticRunDate)
        cls.zeroRecords = rOpenVA.copyVA()

    def test_copyVA_zeroRecords_False(self):
        """Check that copyVA() returns zeroRecords=False."""

        self.assertFalse(self.zeroRecords)

    def test_copyVA_isFile(self):
        """Check that copyVA() brings in new file."""

        self.assertTrue(
            os.path.isfile('OpenVAFiles/openVA_input.csv')
        )

    def test_copyVA_merge(self):
        """Check that copyVA() includes all records."""

        hasAll = True
        with open('OpenVAFiles/pycrossva_input.csv') as fCombined:
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

        os.remove('ODKFiles/odkBCExportPrev.csv')
        os.remove('ODKFiles/odkBCExportNew.csv')
        os.remove('OpenVAFiles/openVA_input.csv')
        os.remove('Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )


class Check_zeroRecords(unittest.TestCase):


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
        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

        # pipelineRunDate = datetime.datetime.now()
        pipelineRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='Pipeline.db',
                            dbDirectory='.',
                            dbKey='enilepiP',
                            plRunDate=pipelineRunDate)
        conn = xferDB.connectDB()
        settingsPipeline = xferDB.configPipeline(conn)
        settingsODK = xferDB.configODK(conn)
        settingsInterVA = xferDB.configOpenVA(conn,
                                              'InterVA',
                                              settingsPipeline.workingDirectory)
        cls.staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')

        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )

        rOpenVA = OpenVA(vaArgs=settingsInterVA,
                         pipelineArgs=settingsPipeline,
                         odkID=settingsODK.odkID,
                         runDate=cls.staticRunDate)
        cls.zeroRecords = rOpenVA.copyVA()

    def test_copyVA_zeroRecords_True(self):
        """Check that copyVA() returns zeroRecords == True."""

        self.assertTrue(self.zeroRecords)

    def test_copyVA_zeroRecords_True_no_file(self):
        """Check that copyVA() does not produce file if zero records."""

        self.assertFalse(
            os.path.isfile('OpenVAFiles/openVA_input.csv')
        )

    @classmethod
    def tearDownClass(cls):

        os.remove('ODKFiles/odkBCExportPrev.csv')
        os.remove('ODKFiles/odkBCExportNew.csv')
        os.remove('Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )


class Check_InSilicoVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('Check_InSilicoVA_Pipeline.db'):
            os.remove('Check_InSilicoVA_Pipeline.db')
        createTransferDB('Check_InSilicoVA_Pipeline.db', '.', 'enilepiP')

        # pipelineRunDate = datetime.datetime.now()
        pipelineRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='Check_InSilicoVA_Pipeline.db',
                            dbDirectory='.',
                            dbKey='enilepiP',
                            plRunDate=pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InSilicoVA', 'InSilicoVA-2016|1.0.0|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        sql = 'UPDATE InSilicoVA_Conf SET data_type = ?'
        par = ('WHO2016',)
        c.execute(sql, par)
        settingsPipeline = xferDB.configPipeline(conn)
        settingsODK = xferDB.configODK(conn)
        settingsInSilicoVA = xferDB.configOpenVA(conn,
                                                 'InSilicoVA',
                                                 settingsPipeline.workingDirectory)
        # conn.rollback()
        conn.close()
        cls.staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                            strftime('%Y_%m_%d_%H:%M:%S')
 
        cls.rScript = os.path.join('OpenVAFiles', cls.staticRunDate,
                                   'Rscript_' + cls.staticRunDate + '.R')
        cls.rOutFile = os.path.join('OpenVAFiles', cls.staticRunDate,
                                    'Rscript_' + cls.staticRunDate + '.Rout')
        rOpenVA = OpenVA(vaArgs=settingsInSilicoVA,
                         pipelineArgs=settingsPipeline,
                         odkID=settingsODK.odkID,
                         runDate=cls.staticRunDate)
        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()
        cls.completed = rOpenVA.getCOD()

    def test_insilicoVA_rscript(self):
        """Check that rScript() creates an R script for InSilicoVA."""

        self.assertTrue(os.path.isfile(self.rScript))

    def test_insilicoVA_rOut(self):
        """Check that getCOD() executes R script for InsilicoVA"""

        self.assertTrue(os.path.isfile(self.rOutFile))

    def test_insilicoVA_EAV(self):
        """Check that getCOD() creates EAV outpue for InsilicoVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/entityAttributeValue.csv'))

    def test_insilicoVA_recordStorage(self):
        """Check that getCOD() recordStorage.csv for InsilicoVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/recordStorage.csv'))

    def test_insilicoVA_returncode(self):
        """Check that getCOD() runs InsilicoVA successfully"""

        self.assertEqual(self.completed.returncode, 0)


    @classmethod
    def tearDownClass(cls):

        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")
        os.remove("Check_InSilicoVA_Pipeline.db")


class Check_InterVA(unittest.TestCase):


    @classmethod
    def setUpClass(cls):

        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/previous_bc_export.csv',
                    'ODKFiles/odkBCExportPrev.csv')
        shutil.copy('ODKFiles/another_bc_export.csv',
                    'ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('Check_InterVA_Pipeline.db'):
            os.remove('Check_InterVA_Pipeline.db')
        createTransferDB('Check_InterVA_Pipeline.db', '.', 'enilepiP')

        # pipelineRunDate = datetime.datetime.now()
        pipelineRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                            strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='Check_InterVA_Pipeline.db',
                            dbDirectory='.',
                            dbKey='enilepiP',
                            plRunDate=pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InterVA','InterVA5|5|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        settingsPipeline = xferDB.configPipeline(conn)
        settingsODK = xferDB.configODK(conn)
        settingsInterVA = xferDB.configOpenVA(conn,
                                              'InterVA',
                                              settingsPipeline.workingDirectory)
        conn.close()
        cls.staticRunDate = datetime.datetime(2018, 9, 1, 9, 0, 0). \
                            strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )
        cls.rScript = os.path.join('OpenVAFiles', cls.staticRunDate,
                                   'Rscript_' + cls.staticRunDate + '.R')
        cls.rOutFile = os.path.join('OpenVAFiles', cls.staticRunDate,
                                    'Rscript_' + cls.staticRunDate + '.Rout')
        rOpenVA = OpenVA(vaArgs=settingsInterVA,
                         pipelineArgs=settingsPipeline,
                         odkID=settingsODK.odkID,
                         runDate=cls.staticRunDate)
        zeroRecords = rOpenVA.copyVA()
        rOpenVA.rScript()
        cls.completed = rOpenVA.getCOD()

    def test_interva_rscript(self):
        """Check that getCOD() executes R script for InterVA"""

        self.assertTrue(os.path.isfile(self.rScript))

    def test_interva_rOut(self):
        """Check that getCOD() executes R script for InterVA"""

        self.assertTrue(os.path.isfile(self.rOutFile))

    def test_interva_EAV(self):
        """Check that getCOD() creates EAV outpue for InterVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/entityAttributeValue.csv'))

    def test_interva_recordStorage(self):
        """Check that getCOD() recordStorage.csv for InterVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/recordStorage.csv'))

    def test_interva_returncode(self):
        """Check that getCOD() runs InterVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        os.remove('Check_InterVA_Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_SmartVA(unittest.TestCase):


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
        if not os.path.isfile('Pipeline.db'):
            createTransferDB('Pipeline.db', '.', 'enilepiP')

        # pipelineRunDate = datetime.datetime.now()
        pipelineRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='copy_Pipeline.db',
                            dbDirectory='.',
                            dbKey='enilepiP',
                            plRunDate=pipelineRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('SmartVA', 'SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1')
        c.execute(sql, par)
        settingsPipeline = xferDB.configPipeline(conn)
        settingsODK = xferDB.configODK(conn)
        settingsSmartVA = xferDB.configOpenVA(conn,
                                              'SmartVA',
                                              settingsPipeline.workingDirectory)
        conn.rollback()
        conn.close()
        cls.staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )
        cliSmartVA = OpenVA(vaArgs=settingsSmartVA,
                            pipelineArgs=settingsPipeline,
                            odkID=settingsODK.odkID,
                            runDate=cls.staticRunDate)
        zeroRecords = cliSmartVA.copyVA()
        cls.completed = cliSmartVA.getCOD()
        cls.svaOut = os.path.join(
            'OpenVAFiles',
            cls.staticRunDate,
            '1-individual-cause-of-death/individual-cause-of-death.csv'
        )

    def test_smartva(self):
        """Check that getCOD() executes smartva cli"""

        self.assertTrue(os.path.isfile(self.svaOut))

    def test_smartva_EAV(self):
        """Check that getCOD() creates EAV outpue for SmartVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/entityAttributeValue.csv'))

    def test_smartva_recordStorage(self):
        """Check that getCOD() recordStorage.csv for SmartVA"""

        self.assertTrue(os.path.isfile('OpenVAFiles/recordStorage.csv'))

    def test_smartva_returncode(self):
        """Check that getCOD() runs SmartVA successfully"""

        self.assertEqual(self.completed.returncode, 0)

    @classmethod
    def tearDownClass(cls):

        os.remove('Pipeline.db')
        shutil.rmtree(
            os.path.join('OpenVAFiles', cls.staticRunDate),
            ignore_errors=True
        )
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_Exceptions_InSilicoVA(unittest.TestCase):


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

       
    def setUp(self):
        
        staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='copy_Pipeline.db',
                                dbDirectory='.',
                                dbKey='enilepiP',
                                plRunDate=staticRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        algorithm = 'InSilicoVA'
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InSilicoVA', 'InSilicoVA|1.1.4|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        sql = 'UPDATE InSilicoVA_Conf SET data_type = ?'
        par = ('WHO2012',)
        c.execute(sql, par)
        settingsPipeline = xferDB.configPipeline(conn)
        settingsAlgorithm = xferDB.configOpenVA(conn,
                                                algorithm,
                                                settingsPipeline.workingDirectory)

        self.rOpenVA = OpenVA(vaArgs=settingsAlgorithm,
                              pipelineArgs=settingsPipeline,
                              odkID=None,
                              runDate=staticRunDate)
        zeroRecords = self.rOpenVA.copyVA()
        self.rOpenVA.rScript()
        conn.rollback()
        conn.close()

    def test_insilico_exception(self):
        """getCOD() raises exception with faulty R script for InSilicoVA."""
        
        self.assertRaises(OpenVAError, self.rOpenVA.getCOD)

    def tearDown(self):
        staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', staticRunDate),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):
    
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_Exceptions_InterVA(unittest.TestCase):


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

       
    def setUp(self):
        
        staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='copy_Pipeline.db',
                                dbDirectory='.',
                                dbKey='enilepiP',
                                plRunDate=staticRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        algorithm = 'InterVA'
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('InterVA', 'InterVA4|4.04|Custom|1|2016 WHO Verbal Autopsy Form|v1_4_1')
        c.execute(sql, par)
        sql = 'UPDATE InterVA_Conf SET version = ?'
        par = ('4',)
        c.execute(sql, par)
        settingsPipeline = xferDB.configPipeline(conn)
        settingsAlgorithm = xferDB.configOpenVA(conn,
                                                algorithm,
                                                settingsPipeline.workingDirectory)

        self.rOpenVA = OpenVA(vaArgs=settingsAlgorithm,
                              pipelineArgs=settingsPipeline,
                              odkID=None,
                              runDate=staticRunDate)
        zeroRecords = self.rOpenVA.copyVA()
        self.rOpenVA.rScript()
        conn.rollback()
        conn.close()

    def test_interva_exception(self):
        """getCOD() should raise an exception with problematic Interva R script."""

        self.assertRaises(OpenVAError, self.rOpenVA.getCOD)

    def tearDown(self):
        staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', staticRunDate),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):
    
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


class Check_Exceptions_SmartVA(unittest.TestCase):


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
       
    def setUp(self):
        
        staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        xferDB = TransferDB(dbFileName='copy_Pipeline.db',
                                dbDirectory='.',
                                dbKey='enilepiP',
                                plRunDate=staticRunDate)
        conn = xferDB.connectDB()
        c = conn.cursor()
        algorithm = 'SmartVA'
        sql = 'UPDATE Pipeline_Conf SET algorithm = ?, algorithmMetadataCode = ?'
        par = ('SmartVA', 'SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1')
        c.execute(sql, par)
        ntSmartVA = collections.namedtuple("ntSmartVA",
                                           ["SmartVA_country",
                                            "SmartVA_hiv",
                                            "SmartVA_malaria",
                                            "SmartVA_hce",
                                            "SmartVA_freetext",
                                            "SmartVA_figures",
                                            "SmartVA_language"])
        settingsAlgorithm = ntSmartVA("Unknown",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong",
                                      "Wrong")
        settingsPipeline = xferDB.configPipeline(conn)

        self.rOpenVA = OpenVA(vaArgs=settingsAlgorithm,
                              pipelineArgs=settingsPipeline,
                              odkID=None,
                              runDate=staticRunDate)
        zeroRecords = self.rOpenVA.copyVA()
        conn.rollback()
        conn.close()

    def test_smartva_exception(self):
        """getCOD() should raise an exception with faulty args for smartva cli"""

        self.assertRaises(SmartVAError, self.rOpenVA.getCOD)

    def tearDown(self):
        staticRunDate = datetime.datetime(
            2018, 9, 1, 9, 0, 0).strftime('%Y_%m_%d_%H:%M:%S')
        shutil.rmtree(
            os.path.join('OpenVAFiles', staticRunDate),
            ignore_errors=True
        )

    @classmethod
    def tearDownClass(cls):
    
        if os.path.isfile('ODKFiles/odkBCExportNew.csv'):
            os.remove('ODKFiles/odkBCExportNew.csv')
        if os.path.isfile('ODKFiles/odkBCExportPrev.csv'):
            os.remove('ODKFiles/odkBCExportPrev.csv')
        if os.path.isfile('OpenVAFiles/openVA_input.csv'):
            os.remove('OpenVAFiles/openVA_input.csv')
        if os.path.isfile("OpenVAFiles/recordStorage.csv"):
            os.remove("OpenVAFiles/recordStorage.csv")
        if os.path.isfile("OpenVAFiles/entityAttributeValue.csv"):
            os.remove("OpenVAFiles/entityAttributeValue.csv")


if __name__ == '__main__':
    unittest.main(verbosity=2)
