#------------------------------------------------------------------------------#
# transferDB.py
#
# Notes
#
# -- exceptions appear at end of file
# 
# (1) This class should have a methods: connect; configODK, configOpenVA,
#     configDHIS2, storeVA, storeBlob, logDB, & logFile
#
#  you could use connect as a context (and thus close it every time)
#  but this might be a waste of resources to open and close everytime?
# 
#------------------------------------------------------------------------------#

import os
import collections
import datetime
from pipeline import PipelineError
from pysqlcipher3 import dbapi2 as sqlcipher

class TransferDB:
    """This class connects to the Transfer database."""


    def __init__(self, dbFileName, dbDirectory, dbKey):

        self.dbFileName = dbFileName
        self.dbDir = dbDirectory
        self.dbKey = dbKey
        self.dbPath = os.path.join(dbDirectory, dbFileName)


    def connectDB(self):
        """Check if DB file is present, then connect and return Connection object.

        Long description.

        Returns
        -------
        Connection
            Used to connect with (encrypted) SQLite database.
                    
        Raises
        ------

        """

        ## Check for DB file.
        dbFilePresent = os.path.isfile(self.dbPath)
        if not dbFilePresent:
            raise DatabaseConnectionError("")

        conn = sqlcipher.connect(self.dbPath)
        parSetKey = "\"" + self.dbKey + "\""
        conn.execute("PRAGMA key = " + parSetKey)

        return(conn)

    def logDB(self):
        pass

    def logFile(self):
        pass

    def configPipeline(self, conn):
        """Grab Pipline configuration settings from DB."""

        c = conn.cursor()

        c.execute("SELECT algorithmMetadataCode FROM Pipeline_Conf;")
        algorithmMetadataCode = c.fetchone()[0]

        c.execute("SELECT codSource FROM Pipeline_Conf;")
        codSource = c.fetchone()[0]
        if codSource not in ("ICD10", "WHO", "Tariff"):
            raise PipelineConfigurationError \
                ("Problem with Pipeline_Conf.codSource")

        c.execute("SELECT algorithm FROM Pipeline_Conf;")
        algorithm = c.fetchone()[0]
        if algorithm not in ("InterVA", "Insilico", "SmartVA", "InterVA5"):
            raise PipelineConfigurationError \
                ("Problem with Pipeline_Conf.algorithm")

        c.execute("SELECT workingDirectory FROM Pipeline_Conf;")
        workingDirectory = c.fetchone()[0]

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
        return(settingsPipeline)

    def configODK(self, conn):
        """Query ODK configuration settings from database.

        This method is intended to be used in conjunction with
        (1) TransferDB.connectDB(), which establishes a connection to a
        database with the Pipeline configuration settings; and (2)
        ODK.connect(), which establishes a connection to an ODK Aggregate
        server.  Thus, ODK.config() gets its input from TransferDB.connectDB()
        and the output from ODK.config() is a valid argument for ODK.config().

        Parameters
        ----------
        conn : sqlite3 Connection object

        Returns
        -------
        tuple
            Contains all parameters for ODK.connect().

        Raises
        ------

        """
        c = conn.cursor()
        sqlODK = "SELECT odkID, odkURL, odkUser, odkPassword, odkFormID, \
          odkLastRun, odkLastRunResult FROM ODK_Conf;"
        queryODK = c.execute(sqlODK).fetchall()
        odkID = queryODK[0][0]
        odkURL = queryODK[0][1]
        odkUser = queryODK[0][2]
        odkPassword = queryODK[0][3]
        odkFormID = queryODK[0][4]
        odkLastRun = queryODK[0][5]
        odkLastRunResult = queryODK[0][6]
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
                                        "odkLastRunResult",
                                        "odkLastRunDate",
                                        "odkLastRunDatePrev"]
        )
        settingsODK = ntODK(odkID,            
                            odkURL,           
                            odkUser,          
                            odkPassword,      
                            odkFormID,        
                            odkLastRun,       
                            odkLastRunResult, 
                            odkLastRunDate,   
                            odkLastRunDatePrev)

        return(settingsODK)

    def configOpenVA(self):
        """Query OpenVA configuration settings from database.

        This method is intended to receive its input (a Connection object) 
        from TransferDB.connectDB(), which establishes a connection to a
        database with the Pipeline configuration settings.  It sets up the
        configuration for all of the VA algorithms included in the R package
        openVA.  The output from configOpenVA() serves as an input to the
        method OpenVA.setAlgorithmParameters()

        Parameters
        ----------
        conn : sqlite3 Connection object

        Returns
        -------
        tuple
            Contains all parameters needed for OpenVA.setAlgorithmParameters().

        Raises
        ------

        """
        pass

    def configDHIS(self):
        pass

    def storeVA(self):
        pass

    def storeBlob(self):
        pass

#------------------------------------------------------------------------------#
# Exceptions
#------------------------------------------------------------------------------#
class DatabaseConnectionError(PipelineError): pass
class PipelineConfigurationError(PipelineError): pass
