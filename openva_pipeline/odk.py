#------------------------------------------------------------------------------#
# odk.py
#
# Notes
#
# (1) Need to start logging information
# (2) Class should have attribute indicating # of records retrieved
# (3) add verbose mode that prints a ton of messages to the console
# (4) add more info about errors, e.g., wrong formID, wrong user id, etc
#
#-----------------------------------------------------------------------------#
import subprocess
from pipeline import PipelineError
from pysqlcipher3 import dbapi2 as sqlcipher

class ODK:
    """Manages Pipeline's interaction with ODK Aggregate.

    This is a longer description.

    Parameters
    ----------
    odkID : VA record ID
    odkURL : URL for ODK Aggregate server

    Raises
    ------
    """


    # def __init__(self, odkID, odkURL, odkUser, odkPassword, odkFormID,
    #              odkLastRun, odkLastRunDate, odkLastRunDatePrev,
    #              odkLastRunResult, bcStorageDir, bcExportDir, bcFileName):

    #     self.odkID = odkID
    #     self.odkURL = odkURL
    #     self.odkUser = odkUser
    #     self.odkPassword = odkPassword
    #     self.odkFormID = odkFormID
    #     self.odkLastRun = odkLastRun
    #     self.odkLastRunDate = odkLastRunDate
    #     self.odkLastRunDatePrev = odkLastRunDatePrev
    #     self.odkLastRunResult = odkLastRunResult

    #     # odk briefcase arguments
    #     # self.bcPath = "libs/ODK-Briefcase-v1.12.0.jar" # need to specify module path
    #     self.bcPath = "libs/ODK-Briefcase-v1.10.1.jar" # need to specify module path
    #     self.bcStorageDir = bcStorageDir
    #     self.bcExportDir = bcExportDir
    #     self.bcFileName = bcFileName

    def connect(self):
        """Export records from ODK Aggregate server using ODK Briefcase.

        Longer description here.

        Returns
        -------
        Connection object
            SQL connection object for querying config settings

        Raises
        ------

        """

        bcArgs = ['java', '-jar', self.bcPath,
                  # '-e', '-oc', '-em', ## '-e' needed for ODK-Briefcase-v1.12.0.jar
                  '-oc', '-em',
                  '-id', str('"' + self.odkFormID    + '"'),
                  '-sd', str('"' + self.bcStorageDir + '"'),
                  '-ed', str('"' + self.bcExportDir  + '"'),
                  '-f',  str('"' + self.bcFileName   + '"'),
                  '-url', self.odkURL,
                  '-u', self.odkUser,
                  '-p', self.odkPassword,
                  '-start', self.odkLastRunDatePrev]

        completed = subprocess.run(args = bcArgs,
                                   stdin  = subprocess.PIPE,
                                   stdout = subprocess.PIPE,
                                   stderr = subprocess.PIPE)
        return(completed)


#------------------------------------------------------------------------------#
# Exceptions
#------------------------------------------------------------------------------#
class DatabaseConnectionError(PipelineError):
    pass
