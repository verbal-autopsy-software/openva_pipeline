"""
openva.transferDB
-----------------

This module handles interactions with the Transfer database.
"""

import os
import shutil
import collections
import datetime
import sqlite3
from pickle import dumps
from pandas import read_csv
from pysqlcipher3 import dbapi2 as sqlcipher

from .exceptions import DatabaseConnectionError
from .exceptions import PipelineConfigurationError
from .exceptions import ODKConfigurationError
from .exceptions import DatabaseConnectionError
from .exceptions import PipelineError
from .exceptions import OpenVAConfigurationError
from .exceptions import DHISConfigurationError

class TransferDB:
    """This class handles interactions with the Transfer database.

    The Pipeline accesses configuration information from the Transfer database,
    and also stores log messages and verbal autopsy records in the DB.  The
    Transfer database is encrypted using sqlcipher3 (and the pysqlcipher3
    module is imported to establish DB connection).

    Parameters

    :param dbFileName: File name of the Tranfser database.
    :type dbFileName: str
    :param dbDirectory: Path of folder containing the Transfer database.
    :type dbDirectory: str
    :param dbKey: Encryption key for the Transfer database.
    :type dbKey: str
    :param plRunDate: Date when pipeline started latest 
      run (YYYY-MM-DD_hh:mm:ss).
    :type plRunDate: date
    """

    def __init__(self, dbFileName, dbDirectory, dbKey, plRunDate):

        self.dbFileName = dbFileName
        self.dbDirectory = dbDirectory
        self.dbKey = dbKey
        self.dbPath = os.path.join(dbDirectory, dbFileName)
        self.workingDirectory = None
        self.plRunDate = plRunDate

    def connectDB(self):
        """Connect to Transfer database.

        Uses parameters supplied to the parent class, TransferDB, to connect to
        the (encrypted) Transfer database.

        :returns: Used to query (encrypted) SQLite database.
        :rtype: SQLite database connection object
        :raises: DatabaseConnectionError
        """

        dbFilePresent = os.path.isfile(self.dbPath)
        if not dbFilePresent:
            raise DatabaseConnectionError("")

        conn = sqlcipher.connect(self.dbPath)
        parSetKey = "\"" + self.dbKey + "\""
        conn.execute("PRAGMA key = " + parSetKey)
        try:
            sqlTestConnection = "SELECT name FROM SQLITE_MASTER \
              where type = 'table';"
            conn.execute(sqlTestConnection)
        except (sqlcipher.DatabaseError) as e:
            raise DatabaseConnectionError("Database password error..." + str(e))

        return(conn)

    def configPipeline(self, conn):
        """Grabs Pipline configuration settings.

        This method queries the Pipeline_Conf table in Transfer database and
        returns a tuple with attributes (1) algorithmMetadataCode; (2)
        codSource; (3) algorithm; and (4) workingDirectory.

        :returns: Arguments needed to configure the OpenVA Pipeline
          alogrithmMetadataCode - attribute describing VA data
          codSource - attribute detailing the source of the Cause of Death list
          algorithm - attribute indicating which VA algorithm to use
          workingDirectory - attribute indicating the working directory
        :rtype: tuple
        :raises: PipelineConfigurationError
        """

        c = conn.cursor()

        try:
            c.execute("SELECT dhisCode from Algorithm_Metadata_Options;")
            metadataQuery = c.fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table Algorithm_Metadata_Options..." +
                 str(e)
                )

        try:
            sqlPipeline = "SELECT algorithmMetadataCode, codSource, algorithm, \
              workingDirectory FROM Pipeline_Conf;"
            queryPipeline = c.execute(sqlPipeline).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table Pipeline_Conf..." + str(e))

        algorithmMetadataCode = queryPipeline[0][0]
        if algorithmMetadataCode not in [j for i in metadataQuery for j in i]:
            raise PipelineConfigurationError \
                ("Problem in database: Pipeline_Conf.algorithmMetadataCode")
        codSource = queryPipeline[0][1]
        if codSource not in ("ICD10", "WHO", "Tariff"):
            raise PipelineConfigurationError \
                ("Problem in database: Pipeline_Conf.codSource")
        algorithm = queryPipeline[0][2]
        if algorithm not in ("InterVA", "InSilicoVA", "SmartVA"):
            raise PipelineConfigurationError \
                ("Problem in database: Pipeline_Conf.algorithm")
        workingDirectory = queryPipeline[0][3]
        if not os.path.isdir(workingDirectory):
            raise PipelineConfigurationError \
                ("Problem in database: Pipeline_Conf.workingDirectory")

        ntPipeline = collections.namedtuple("ntPipeline",
                                            ["algorithmMetadataCode",
                                             "codSource",
                                             "algorithm",
                                             "workingDirectory"]
        )
        settingsPipeline = ntPipeline(algorithmMetadataCode,
                                      codSource,
                                      algorithm,
                                      workingDirectory)
        self.workingDirectory = workingDirectory
        return(settingsPipeline)

    def configODK(self, conn):
        """Query ODK configuration settings from database.

        This method is intended to be used in conjunction with (1)
        :meth:`TransferDB.connectDB() <connectDB>`, which establishes a 
        connection to a database with the Pipeline configuration settings; 
        and (2) :meth:`ODK.briefcase() <openva_pipeline.odk.ODK.briefcase>`, 
        which establishes a connection to an ODK Aggregate server.  Thus,
        TransferDB.configODK() gets its input from 
        :meth:`TransferDB.connectDB() <connectDB>` and the output from 
        TransferDB.configODK() is a valid argument for 
        :meth:`ODK.briefcase()<openva_pipeline.odk.ODK.briefcase>`.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :returns: Contains all parameters for 
          :meth:`ODK.briefcase()<openva_pipeline.odk.ODK.briefcase>`.
        :rtype: tuple
        :raises: ODKConfigurationError
        """

        c = conn.cursor()
        sqlODK = "SELECT odkID, odkURL, odkUser, odkPassword, odkFormID, \
          odkLastRun FROM ODK_Conf;"
        try:
            queryODK = c.execute(sqlODK).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise ODKConfigurationError \
                ("Problem in database table ODK_Conf..." + str(e))
        odkID = queryODK[0][0]
        odkURL = queryODK[0][1]
        startHTML = odkURL[0:7]
        startHTMLS = odkURL[0:8]
        if not (startHTML == "http://" or startHTMLS == "https://"):
            raise ODKConfigurationError \
                ("Problem in database: ODK_Conf.odkURL")
        odkUser = queryODK[0][2]
        odkPassword = queryODK[0][3]
        odkFormID = queryODK[0][4]
        odkLastRun = queryODK[0][5]
        # odkLastRunResult = queryODK[0][6]
        # if not odkLastRunResult in ("success", "fail"):
        #     raise ODKConfigurationError \
        #         ("Problem in database: ODK_Conf.odkLastRunResult")
        odkLastRunDate = datetime.datetime.strptime(odkLastRun,
                                                    "%Y-%m-%d_%H:%M:%S"
                                                   ).strftime("%Y/%m/%d")
        odkLastRunDatePrev = (datetime.datetime.strptime(odkLastRunDate,
                                                         "%Y/%m/%d") - \
                              datetime.timedelta(days=1)).strftime("%Y/%m/%d")

        ntODK = collections.namedtuple("ntODK",
                                       ["odkID",
                                        "odkURL",
                                        "odkUser",
                                        "odkPassword",
                                        "odkFormID",
                                        "odkLastRun",
                                        # "odkLastRunResult",
                                        "odkLastRunDate",
                                        "odkLastRunDatePrev"]
        )
        settingsODK = ntODK(odkID,
                            odkURL,
                            odkUser,
                            odkPassword,
                            odkFormID,
                            odkLastRun,
                            # odkLastRunResult,
                            odkLastRunDate,
                            odkLastRunDatePrev)

        return(settingsODK)

    def updateODKLastRun(self, conn, plRunDate):
        """Update Transfer Database table ODK_Conf.odkLastRun

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :param plRunDate: Date when pipeline started latest run 
        :type plRunDate: date (YYYY-MM-DD_hh:mm:ss)
        """

        c = conn.cursor()
        sql = "UPDATE ODK_Conf SET odkLastRun = ?"
        par = (plRunDate,)
        c.execute(sql, par)
        conn.commit()

    def checkDuplicates(self, conn):
        """Search for duplicate VA records.

        This method searches for duplicate VA records in ODK Briefcase export
        file and the Tranfser DB.  If duplicates are found, a warning message
        is logged to the EventLog table in the Transfer database.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :raises: DatabaseConnectionError, PipelineError
        """

        if self.workingDirectory == None:
            raise PipelineError("Need to run configPipeline.")
        c = conn.cursor()
        odkBCExportPath = os.path.join(self.workingDirectory,
                                       "ODKFiles",
                                       "odkBCExportNew.csv")
        dfODK = read_csv(odkBCExportPath)
        dfODKID = dfODK["meta-instanceID"]

        sql = "SELECT id FROM VA_Storage"
        c.execute(sql)
        vaIDs = c.fetchall()
        vaIDsList = [j for i in vaIDs for j in i]
        vaDuplicates = set(dfODKID).intersection(set(vaIDsList))
        timeFMT = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        if len(vaDuplicates) > 0:
            nDuplicates = len(vaDuplicates)
            sqlXferDB = "INSERT INTO EventLog \
                         (eventDesc, eventType, eventTime) VALUES (?, ?, ?)"
            eventDescPart1 = ["Duplicate record with ODK Meta-Instance ID: "] \
                            * nDuplicates
            eventDescPart2 = list(vaDuplicates)
            eventDesc = [x + y for x, y in zip(eventDescPart1, eventDescPart2)]
            eventType = ["Warning"] * nDuplicates
            eventTime = [timeFMT] * nDuplicates
            par = list(zip(eventDesc, eventType, eventTime))
            c.executemany(sqlXferDB, par)
            conn.commit()

    def configOpenVA(self, conn, algorithm, pipelineDir):
        """Query OpenVA configuration settings from database.

        This method is intended to receive its input (a Connection object) 
        from :meth:`TransferDB.connectDB() <connectDB>`, which establishes a 
        connection to a database with the Pipeline configuration settings.  It 
        sets up the configuration for all of the VA algorithms included in the 
        R package openVA.  The output from 
        :meth:`configOpenVA() <configOpenVA>` serves as an input to the method 
        :meth:`OpenVA.setAlgorithmParameters() <openva_pipeline.odk.ODK.setAlgorithmParameters>`.  
        This is a wrapper function that calls :meth:`configInterVA`,
        :meth:`configInSilicoVA`, and :meth:`configSmartVA` to actually pull 
        configuration settings from the database.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :param algorithm: VA algorithm used by R package openVA
        :type algorithm: str
        :param pipelineDir: Working directory for the Pipeline
        :type pipelineDir: str
        :returns: Contains all parameters needed for 
          OpenVA.setAlgorithmParameters().
        :rtypes: tuple
        :raises: OpenVAConfigurationError
        """

        if algorithm == "InterVA":
            settingsInterVA = self._configInterVA(conn, pipelineDir)
            return(settingsInterVA)
        elif algorithm == "InSilicoVA":
            settingsInSilicoVA = self._configInSilicoVA(conn, pipelineDir)
            return(settingsInSilicoVA)
        elif algorithm == "SmartVA":
            settingsSmartVA = self._configSmartVA(conn, pipelineDir)
            return(settingsSmartVA)
        else:
            raise PipelineConfigurationError \
                ("Not an acceptable parameter for 'algorithm'.")

    def _configInterVA(self, conn, pipelineDir):
        """Query OpenVA configuration settings from database.

        This method is called by configOpenVA when the VA algorithm is either
        InterVA4 or InterVA5.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :param pipelineDir: Working directory for the Pipeline
        :type pipelineDir: str
        :returns: Contains all parameters needed for 
          OpenVA.setAlgorithmParameters().
        :rtype: tuple
        :raises: OpenVAConfigurationError
        """

        c = conn.cursor()

        try:
            sqlInterVA = "SELECT version, HIV, Malaria FROM InterVA_Conf;"
            queryInterVA = c.execute(sqlInterVA).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table InterVA_Conf..." + str(e))

        # Database Table: InterVA_Conf
        intervaVersion = queryInterVA[0][0]
        if not intervaVersion in ("4", "5"):
            raise OpenVAConfigurationError \
                ("Problem in database: InterVA_Conf.version \
                (valid options: '4' or '5').")
        intervaHIV = queryInterVA[0][1]
        if not intervaHIV in ("v", "l", "h"):
            raise OpenVAConfigurationError \
                ("Problem in database: InterVA_Conf.HIV \
                (valid options: 'v', 'l', or 'h').")
        intervaMalaria = queryInterVA[0][2]
        if not intervaMalaria in ("v", "l", "h"):
            raise OpenVAConfigurationError \
                ("Problem in database: InterVA_Conf.Malaria \
                (valid options: 'v', 'l', or 'h').")

        # Database Table: Advanced_InterVA_Conf
        try:
            sqlAdvancedInterVA = "SELECT output, append, \
            groupcode, replicate, replicate_bug1, replicate_bug2 \
            FROM Advanced_InterVA_Conf;"
            queryAdvancedInterVA = c.execute(sqlAdvancedInterVA).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table Advanced_InterVA_Conf..." + str(e))

        intervaOutput = queryAdvancedInterVA[0][0]
        if not intervaOutput in ("classic", "extended"):
            raise OpenVAConfigurationError \
                ("Problem in database: Advanced_InterVA_Conf.output.")
        intervaAppend = queryAdvancedInterVA[0][1]
        if not intervaAppend in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: Advanced_InterVA_Conf.append.")
        intervaGroupcode = queryAdvancedInterVA[0][2]
        if not intervaGroupcode in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: Advanced_InterVA_Conf.groupcode.")
        intervaReplicate = queryAdvancedInterVA[0][3]
        if not intervaReplicate in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: Advanced_InterVA_Conf.replicate.")
        intervaReplicateBug1 = queryAdvancedInterVA[0][4]
        if not intervaReplicateBug1 in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: Advanced_InterVA_Conf.replicate_bug1.")
        intervaReplicateBug2 = queryAdvancedInterVA[0][5]
        if not intervaReplicateBug2 in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: Advanced_InterVA_Conf.replicate_bug2.")

        ntInterVA = collections.namedtuple("ntInterVA",
                                           ["InterVA_Version",
                                            "InterVA_HIV",
                                            "InterVA_Malaria",
                                            "InterVA_output",
                                            "InterVA_append",
                                            "InterVA_groupcode",
                                            "InterVA_replicate",
                                            "InterVA_replicate_bug1",
                                            "InterVA_replicate_bug2"]
        )
        settingsInterVA = ntInterVA(intervaVersion,
                                    intervaHIV,
                                    intervaMalaria,
                                    intervaOutput,
                                    intervaAppend,
                                    intervaGroupcode,
                                    intervaReplicate,
                                    intervaReplicateBug1,
                                    intervaReplicateBug2)
        return(settingsInterVA)

    def _configInSilicoVA(self, conn, pipelineDir):
        """Query OpenVA configuration settings from database.

        This method is called by configOpenVA when the VA algorithm is
        InSilicoVA.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :param pipelineDir: Working directory for the Pipeline
        :type pipelineDir: str
        :returns: Contains all parameters needed for 
          OpenVA.setAlgorithmParameters().
        :rtype: tuple
        :raises: OpenVAConfigurationError
        """

        c = conn.cursor()

        # Database Table: InSilicoVA_Conf
        try:
            sqlInSilicoVA = "SELECT data_type, Nsim FROM InSilicoVA_Conf;"
            queryInSilicoVA = c.execute(sqlInSilicoVA).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table InSilicoVA_Conf..." + str(e))

        insilicovaDataType = queryInSilicoVA[0][0]
        if not insilicovaDataType in ("WHO2012", "WHO2016"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.data_type \
                (valid options: 'WHO2012' or 'WHO2016').")
        insilicovaNsim = queryInSilicoVA[0][1]
        if insilicovaNsim in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.Nsim")

        # Database Table: Advanced_InSilicoVA_Conf
        try:
            sqlAdvancedInSilicoVA = "SELECT isNumeric, updateCondProb, \
              keepProbbase_level, CondProb, CondProbNum, datacheck, \
              datacheck_missing, external_sep, thin, \
              burnin, auto_length, conv_csmf, jump_scale, levels_prior, \
              levels_strength, trunc_min, trunc_max, subpop, java_option, seed, \
              phy_code, phy_cat, phy_unknown, phy_external, phy_debias, \
              exclude_impossible_cause, no_is_missing, indiv_CI, groupcode \
              FROM Advanced_InSilicoVA_Conf;"
            queryAdvancedInSilicoVA = c.execute(sqlAdvancedInSilicoVA).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table Advanced_InSilicoVA_Conf..." + str(e))

        insilicovaIsNumeric = queryAdvancedInSilicoVA[0][0]
        if not insilicovaIsNumeric in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.isNumeric \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaUpdateCondProb = queryAdvancedInSilicoVA[0][1]
        if not insilicovaUpdateCondProb in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.updateCondProb \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaKeepProbbaseLevel = queryAdvancedInSilicoVA[0][2]
        if not insilicovaKeepProbbaseLevel in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.keepProbbase_level \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaCondProb = queryAdvancedInSilicoVA[0][3]
        if insilicovaCondProb in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.CondProb \
                (valid options: name of R object).")
        insilicovaCondProbNum = queryAdvancedInSilicoVA[0][4]
        if not insilicovaCondProbNum == "NULL":
            try:
                floatCondProbNum = float(insilicovaCondProbNum)
                validCondProbNum = (0 <= floatCondProbNum <= 1)
            except:
                validCondProbNum = False
                raise OpenVAConfigurationError \
                    ("Problem in database: InSilicoVA_Conf.CondProbNum \
                    (must be between '0' and '1').")
            if not validCondProbNum:
                raise OpenVAConfigurationError \
                    ("Problem in database: InSilicoVA_Conf.CondProbNum \
                    (must be between '0' and '1').")
        insilicovaDatacheck = queryAdvancedInSilicoVA[0][5]
        if not insilicovaDatacheck in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.datacheck \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaDatacheckMissing = queryAdvancedInSilicoVA[0][6]
        if not insilicovaDatacheckMissing in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.datacheck_missing \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaExternalSep = queryAdvancedInSilicoVA[0][7]
        if not insilicovaExternalSep in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.external_sep \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaThin = queryAdvancedInSilicoVA[0][8]
        try:
            thinFloat = float(insilicovaThin)
            validThin = (0 < thinFloat)
        except:
            validThin = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.thin \
                (must be 'thin' > 0.")
        if not validThin:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.thin \
                (must be 'thin' > 0.")
        insilicovaBurnin = queryAdvancedInSilicoVA[0][9]
        try:
            burninFloat = float(insilicovaBurnin)
            validBurnin = (0 < burninFloat)
        except:
            validBurnin = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.burnin \
                (must be 'burnin' > 0.")
        if not validBurnin:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.burnin \
                (must be 'burnin' > 0.")
        insilicovaAutoLength = queryAdvancedInSilicoVA[0][10]
        if not insilicovaAutoLength in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.auto_length \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaConvCSMF = queryAdvancedInSilicoVA[0][11]
        try:
            floatConvCSMF = float(insilicovaConvCSMF)
            validConvCSMF = (0 <= floatConvCSMF <= 1)
        except:
            validConvCSMF = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.conv_csmf \
                (must be between '0' and '1').")
        if not validConvCSMF:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.conv_csmf \
                (must be between '0' and '1').")
        insilicovaJumpScale = queryAdvancedInSilicoVA[0][12]
        try:
            floatJumpScale = float(insilicovaJumpScale)
            validJumpScale = (0 < floatJumpScale)
        except:
            validJumpScale = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.jump_scale \
                (must be greater than '0').")
        if not validJumpScale:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.jump_scale \
                (must be greater than '0').")
        insilicovaLevelsPrior = queryAdvancedInSilicoVA[0][13]
        if insilicovaLevelsPrior in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.levels_prior \
                (valid options: name of R object).")
        insilicovaLevelsStrength = queryAdvancedInSilicoVA[0][14]
        try:
            floatLevelsStrength = float(insilicovaLevelsStrength)
            validLevelsStrength = (0 < floatLevelsStrength)
        except:
            validLevelsStrength = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.levels_strength \
                (must be greater than '0').")
        if not validLevelsStrength:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.levels_strength \
                (must be greater than '0').")
        insilicovaTruncMin = queryAdvancedInSilicoVA[0][15]
        try:
            floatTruncMin = float(insilicovaTruncMin)
            validTruncMin = (0 <= floatTruncMin <= 1)
        except:
            validTruncMin = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.trunc_min \
                (must be between '0' and '1').")
        if not validTruncMin:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.trunc_min \
                (must be between '0' and '1').")
        insilicovaTruncMax = queryAdvancedInSilicoVA[0][16]
        try:
            floatTruncMax = float(insilicovaTruncMax)
            validTruncMax = (0 <= floatTruncMax <= 1)
        except:
            validTruncMax = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.trunc_max \
                (must be between '0' and '1').")
        if not validTruncMax:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.trunc_max \
                (must be between '0' and '1').")
        insilicovaSubpop = queryAdvancedInSilicoVA[0][17]
        if insilicovaSubpop in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.subpop \
                (valid options: name of R object).")
        insilicovaJavaOption = queryAdvancedInSilicoVA[0][18]
        if insilicovaJavaOption == "" or len(insilicovaJavaOption) < 6:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.java_option \
                (should look like '-Xmx1g').")
        joLength = len(insilicovaJavaOption)
        joLastChar = insilicovaJavaOption[(joLength - 1)]
        joFirst4Char = insilicovaJavaOption[0:4]
        joMemSize = insilicovaJavaOption[4:(joLength - 1)]
        if not joFirst4Char == "-Xmx":
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.java_option \
                (should start with '-Xmx').")
        if not joLastChar in ("m", "g"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.java_option \
                (should end with 'g' for gigabyts or 'm' for megabytes).")
        try:
            float_joMemSize = float(joMemSize)
            valid_joMemSize = (0 < float_joMemSize)
        except:
            valid_joMemSize = False
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.java_option \
                (should look like '-Xmx1g').")
        if not valid_joMemSize:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.java_option \
                (should look like '-Xmx1g').")
        insilicovaSeed = queryAdvancedInSilicoVA[0][19]
        try:
            floatSeed = float(insilicovaSeed)
        except:
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.seed \
                (must be between a number; preferably an integer).")
        insilicovaPhyCode = queryAdvancedInSilicoVA[0][20]
        if insilicovaPhyCode in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.phy_code \
                (valid options: name of R object).")
        insilicovaPhyCat = queryAdvancedInSilicoVA[0][21]
        if insilicovaPhyCat in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.phy_cat \
                (valid options: name of R object).")
        insilicovaPhyUnknown = queryAdvancedInSilicoVA[0][22]
        if insilicovaPhyUnknown in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.phy_unknown \
                (valid options: name of R object).")
        insilicovaPhyExternal = queryAdvancedInSilicoVA[0][23]
        if insilicovaPhyExternal in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.phy_external \
                (valid options: name of R object).")
        insilicovaPhyDebias = queryAdvancedInSilicoVA[0][24]
        if insilicovaPhyDebias in ("", None):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.phy_debias \
                (valid options: name of R object).")
        insilicovaExcludeImpossibleCause = queryAdvancedInSilicoVA[0][25]
        if not insilicovaExcludeImpossibleCause in \
          ("subset", "all", "InterVA", "none"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.exclude_impossible_cause \
                (valid options: 'subset', 'all', 'InterVA', and 'none').")
        insilicovaNoIsMissing = queryAdvancedInSilicoVA[0][26]
        if not insilicovaNoIsMissing in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.no_is_missing \
                (valid options: 'TRUE' or 'FALSE').")
        insilicovaIndivCI = queryAdvancedInSilicoVA[0][27]
        if not insilicovaIndivCI == "NULL":
            try:
                floatIndivCI = float(insilicovaIndivCI)
                validIndivCI = (0 < floatIndivCI < 1)
            except:
                validIndivCI = False
                raise OpenVAConfigurationError \
                    ("Problem in database: InSilicoVA_Conf.indiv_CI \
                    (must be between '0' and '1').")
            if not validIndivCI:
                raise OpenVAConfigurationError \
                    ("Problem in database: InSilicoVA_Conf.indiv_CI \
                    (must be between '0' and '1').")
        insilicovaGroupcode = queryAdvancedInSilicoVA[0][28]
        if not insilicovaGroupcode in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError \
                ("Problem in database: InSilicoVA_Conf.groupcode \
                (valid options: 'TRUE' or 'FALSE').")

        ntInSilicoVA = collections.namedtuple("ntInSilicoVA",
                                              ["InSilicoVA_data_type",
                                               "InSilicoVA_Nsim",
                                               "InSilicoVA_isNumeric",
                                               "InSilicoVA_updateCondProb",
                                               "InSilicoVA_keepProbbase_level",
                                               "InSilicoVA_CondProb",
                                               "InSilicoVA_CondProbNum",
                                               "InSilicoVA_datacheck",
                                               "InSilicoVA_datacheck_missing",
                                               "InSilicoVA_external_sep",
                                               "InSilicoVA_thin",
                                               "InSilicoVA_burnin",
                                               "InSilicoVA_auto_length",
                                               "InSilicoVA_conv_csmf",
                                               "InSilicoVA_jump_scale",
                                               "InSilicoVA_levels_prior",
                                               "InSilicoVA_levels_strength",
                                               "InSilicoVA_trunc_min",
                                               "InSilicoVA_trunc_max",
                                               "InSilicoVA_subpop",
                                               "InSilicoVA_java_option",
                                               "InSilicoVA_seed",
                                               "InSilicoVA_phy_code",
                                               "InSilicoVA_phy_cat",
                                               "InSilicoVA_phy_unknown",
                                               "InSilicoVA_phy_external",
                                               "InSilicoVA_phy_debias",
                                               "InSilicoVA_exclude_impossible_cause",
                                               "InSilicoVA_no_is_missing",
                                               "InSilicoVA_indiv_CI",
                                               "InSilicoVA_groupcode"]
        )
        settingsInSilicoVA = ntInSilicoVA(insilicovaDataType,
                                          insilicovaNsim,
                                          insilicovaIsNumeric,
                                          insilicovaUpdateCondProb,
                                          insilicovaKeepProbbaseLevel,
                                          insilicovaCondProb,
                                          insilicovaCondProbNum,
                                          insilicovaDatacheck,
                                          insilicovaDatacheckMissing,
                                          insilicovaExternalSep,
                                          insilicovaThin,
                                          insilicovaBurnin,
                                          insilicovaAutoLength,
                                          insilicovaConvCSMF,
                                          insilicovaJumpScale,
                                          insilicovaLevelsPrior,
                                          insilicovaLevelsStrength,
                                          insilicovaTruncMin,
                                          insilicovaTruncMax,
                                          insilicovaSubpop,
                                          insilicovaJavaOption,
                                          insilicovaSeed,
                                          insilicovaPhyCode,
                                          insilicovaPhyCat,
                                          insilicovaPhyUnknown,
                                          insilicovaPhyExternal,
                                          insilicovaPhyDebias,
                                          insilicovaExcludeImpossibleCause,
                                          insilicovaNoIsMissing,
                                          insilicovaIndivCI,
                                          insilicovaGroupcode)
        return(settingsInSilicoVA)

    def _configSmartVA(self, conn, pipelineDir):
        """Query OpenVA configuration settings from database.

        This method is called by configOpenVA when the VA algorithm is
        SmartVA.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :param pipelineDir: Working directory for the Pipeline
        :type pipelineDir: str
        :returns: Contains all parameters needed for OpenVA.setAlgorithmParameters().
        :rtype: tuple
        :raises: OpenVAConfigurationError
        """

        c = conn.cursor()

        try:
            sqlSmartVA = "SELECT country, hiv, malaria, hce, freetext, figures, \
            language FROM SmartVA_Conf;"
            querySmartVA = c.execute(sqlSmartVA).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table SmartVA_Conf..." + str(e))

        try:
            sqlCountryList = "SELECT abbrev FROM SmartVA_Country;"
            queryCountryList = c.execute(sqlCountryList).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table SmartVA_Country..." + str(e))

        smartvaCountry = querySmartVA[0][0]
        if smartvaCountry not in [j for i in queryCountryList for j in i]:
            raise OpenVAConfigurationError \
                ("Problem in database: SmartVA_Conf.country")
        smartvaHIV = querySmartVA[0][1]
        if not smartvaHIV in ("True", "False"):
            raise OpenVAConfigurationError \
                ("Problem in database: SmartVA_Conf.hiv")
        smartvaMalaria = querySmartVA[0][2]
        if not smartvaMalaria in ("True", "False"):
            raise OpenVAConfigurationError \
                ("Problem in database: SmartVA_Conf.malaria")
        smartvaHCE = querySmartVA[0][3]
        if not smartvaHCE in ("True", "False"):
            raise OpenVAConfigurationError \
                ("Problem in database: SmartVA_Conf.hce")
        smartvaFreetext = querySmartVA[0][4]
        if not smartvaFreetext in ("True", "False"):
            raise OpenVAConfigurationError \
                ("Problem in database: SmartVA_Conf.freetext")
        smartvaFigures = querySmartVA[0][5]
        if not smartvaFigures in ("True", "False"):
            raise OpenVAConfigurationError \
                ("Problem in database: SmartVA_Conf.figures")
        smartvaLanguage = querySmartVA[0][6]
        if not smartvaLanguage in ("english", "chinese", "spanish"):
            raise OpenVAConfigurationError \
                ("Problem in database: SmartVA_Conf.language")

        ntSmartVA = collections.namedtuple("ntSmartVA",
                                           ["SmartVA_country",
                                            "SmartVA_hiv",
                                            "SmartVA_malaria",
                                            "SmartVA_hce",
                                            "SmartVA_freetext",
                                            "SmartVA_figures",
                                            "SmartVA_language"]
        )
        settingsSmartVA = ntSmartVA(smartvaCountry,
                                    smartvaHIV,
                                    smartvaMalaria,
                                    smartvaHCE,
                                    smartvaFreetext,
                                    smartvaFigures,
                                    smartvaLanguage)
        return(settingsSmartVA)

    def configDHIS(self, conn, algorithm):
        """Query DHIS configuration settings from database.

        This method is intended to be used in conjunction with (1)
        :meth:`TransferDB.connectDB()<connectDB>`, which establishes a 
        connection to a database with the Pipeline configuration settings; 
        and (2) :meth:`DHIS.connect()<openva_pipeline.dhis.DHIS.connect>`, 
        which establishes a connection to a DHIS server.  Thus, 
        TransferDB.configDHIS() gets its input from 
        :meth:`TransferDB.connectDB()<connectDB>` and the output from 
        TransferDB.config() is a valid argument for
        :meth:`DHIS.connect()<openva_pipeline.dhis.DHIS.connect>`

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :param algorithm: VA algorithm used by R package openVA
        :type algorithm: str
        :returns: Contains all parameters for 
          :meth:`DHIS.connect() <openva_pipeline.dhis.DHIS.connect>`.
        :rtype: tuple
        :raises: DHISConfigurationError
        """
        c = conn.cursor()
        try:
            sqlDHIS = "SELECT dhisURL, dhisUser, dhisPassword, dhisOrgUnit \
              FROM DHIS_Conf;"
            queryDHIS = c.execute(sqlDHIS).fetchall()
        except (sqlcipher.OperationalError) as e:
            raise PipelineConfigurationError \
                ("Problem in database table DHIS_Conf..." + str(e))

        if algorithm == "SmartVA":
            sqlCODCodes = "SELECT codName, codCode FROM COD_Codes_DHIS WHERE codSource = 'Tariff'"
        else:
            sqlCODCodes = "SELECT codName, codCode FROM COD_Codes_DHIS WHERE codSource = 'WHO'"
        try:
            queryCODCodes = c.execute(sqlCODCodes).fetchall()
            dhisCODCodes = dict(queryCODCodes)
        except (sqlcipher.OperationalError) as e:
            raise DHISConfigurationError \
                ("Problem in database table COD_Codes_DHIS..." + str(e))

        dhisURL = queryDHIS[0][0]
        startHTML = dhisURL[0:7]
        startHTMLS = dhisURL[0:8]
        if not (startHTML == "http://" or startHTMLS == "https://"):
            raise DHISConfigurationError \
                ("Problem in database: DHIS_Conf.dhisURL")
        dhisUser = queryDHIS[0][1]
        if dhisUser == "" or dhisUser == None:
            raise DHISConfigurationError \
                ("Problem in database: DHIS_Conf.dhisUser (is empty)")
        dhisPassword = queryDHIS[0][2]
        if dhisPassword == "" or dhisPassword == None:
            raise DHISConfigurationError \
                ("Problem in database: DHIS_Conf.dhisPassword (is empty)")
        dhisOrgUnit = queryDHIS[0][3]
        if dhisOrgUnit == "" or dhisOrgUnit == None:
            raise DHISConfigurationError \
                ("Problem in database: DHIS_Conf.dhisOrgUnit (is empty)")

        ntDHIS = collections.namedtuple("ntDHIS",
                                       ["dhisURL",
                                        "dhisUser",
                                        "dhisPassword",
                                        "dhisOrgUnit"]
        )
        settingsDHIS = ntDHIS(dhisURL,
                              dhisUser,
                              dhisPassword,
                              dhisOrgUnit)

        return([settingsDHIS, dhisCODCodes])

    def storeVA(self, conn):
        """Store VA records in Transfer database.

        This method is intended to be used in conjunction with the 
        :class:`DHIS <openva_pipeline.dhis.DHIS>` class, which prepares the 
        records into the proper format for storage in the Transfer database.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connectDB() <connectDB>`.)
        :type conn: sqlite3 Connection object
        :raises: PipelineError, DatabaseConnectionError
        """

        if self.workingDirectory == None:
            raise PipelineError("Need to run configPipeline.")
        c = conn.cursor()
        newStoragePath = os.path.join(self.workingDirectory,
                                      "OpenVAFiles",
                                      "newStorage.csv")
        dfNewStorage = read_csv(newStoragePath)
        dfNewStorageID = dfNewStorage["odkMetaInstanceID"]
        timeFMT = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        try:
            for row in dfNewStorage.itertuples():
                xferDBID = row[1]
                nElements = len(row) - 1
                xferDBOutcome = row[nElements]
                vaData = row[1], row[8:(nElements - 1)]
                vaDataFlat = tuple(
                    [y for x in vaData
                     for y in (x if isinstance(x, tuple) else (x,))]
                )
                xferDBRecord = dumps(vaDataFlat)
                sqlXferDB = "INSERT INTO VA_Storage \
                             (id, outcome, record, dateEntered) \
                             VALUES (?, ?, ?, ?)"
                par = [xferDBID, xferDBOutcome,
                       sqlite3.Binary(xferDBRecord), timeFMT]
                c.execute(sqlXferDB, par)
            conn.commit()
        except:
            raise DatabaseConnectionError\
                ("Problem storing VA record to Transfer DB.")

    def makePipelineDirs(self):
        """Create directories for storing files (if they don't exist).

        The method creates the following folders in the working directory (as
        set in the Transfer database table Pipeline_Conf): (1) ODKFiles for
        files containing verbal autopsy records from the ODK Aggregate server; 
        (2) OpenVAFiles containing R scripts and results from the cause 
        assignment algorithms; and (3) DHIS for holding blobs that will be 
        stored in a data repository (DHIS2 server and/or the local Transfer 
        database).

        :raises: PipelineError
        """

        odkPath = os.path.join(self.workingDir, "ODKFiles")
        openVAPath = os.path.join(self.workingDir, "OpenVAFiles")
        dhisPath = os.path.join(self.workingDir, "DHIS")

        if not os.path.isdir(odkPath):
            try:
                os.makedirs(odkPath)
            except OSError as e:
                raise PipelineError("Unable to create directory " + odkPath)
        if not os.path.isdir(openVAPath):
            try:
                os.makedirs(openVAPath)
            except OSError as e:
                raise PipelineError("Unable to create directory " + openVAPath)
        if not os.path.isdir(dhisPath):
            try:
                os.makedirs(dhisPath)
            except OSError as e:
                raise PipelineError("Unable to create directory " + dhisPath)

    def cleanODK(self):
        """Remove ODK Briefcase Export files."""

        if self.workingDirectory == None:
            raise PipelineError("Need to run configPipeline.")
        odkExportNewPath = os.path.join(self.workingDirectory,
                                        "ODKFiles",
                                        "odkBCExportNew.csv")
        odkExportPrevPath = os.path.join(self.workingDirectory,
                                         "ODKFiles",
                                         "odkBCExportPrev.csv")
        if os.path.isfile(odkExportNewPath):
            os.remove(odkExportNewPath)
        if os.path.isfile(odkExportPrevPath):
            os.remove(odkExportPrevPath)


    def cleanOpenVA(self):
        """Remove openVA files with COD results."""

        if self.workingDirectory == None:
            raise PipelineError("Need to run configPipeline.")
        openVAInputPath = os.path.join(self.workingDirectory,
                                        "OpenVAFiles",
                                        "openVA_input.csv")
        recordStoragePath = os.path.join(self.workingDirectory,
                                         "OpenVAFiles",
                                         "recordStorage.csv")
        newStoragePath = os.path.join(self.workingDirectory,
                                      "OpenVAFiles",
                                      "newStorage.csv")
        evaPath = os.path.join(self.workingDirectory,
                               "OpenVAFiles",
                               "entityAttributeValue.csv")
        if os.path.isfile(openVAInputPath):
            os.remove(openVAInputPath)
        if os.path.isfile(recordStoragePath):
            os.remove(recordStoragePath)
        if os.path.isfile(newStoragePath):
            os.remove(newStoragePath)
        if os.path.isfile(evaPath):
            os.remove(evaPath)

    def cleanDHIS(self):
        """Remove DHIS2 blob files."""

        if self.workingDirectory == None:
            raise PipelineError("Need to run configPipeline.")
        shutil.rmtree(
            os.path.join(self.workingDirectory, "DHIS", "blobs")
        )
