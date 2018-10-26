#------------------------------------------------------------------------------#
# odk.py
#
# Notes
#
# (1) Need to start logging information
# (2) Class should have attribute indicating # of records retrieved
# (3) add verbose mode that prints a ton of messages to the console
#
#-----------------------------------------------------------------------------#
import subprocess
import os
import shutil
from transferDB import PipelineError
from pysqlcipher3 import dbapi2 as sqlcipher

class ODK:
    """Manages Pipeline's interaction with ODK Aggregate.

    This class handles the segment of the pipeline related to ODK.  The
    ODK.connect() method calls ODK Briefcase to connect with an ODK Aggregate
    server and export VA records.  It also checks for previously exported files
    and updates them as needed.  Finally, it logs messages and errors to the
    pipeline database.

    Parameters
    ----------
    odkSettings : (named) tuple with all of configuration settings as
        attributes.
    workingDirectory : Directory where openVA Pipeline should create files.

    Methods
    -------
    briefcase(self)
        Uses ODK Briefcase to export VA records from ODK Aggregate server.

    mergePrevExport(self)
        Merge ODK Briefcase export files (if they exist).

    Raises
    ------
    ODKBriefcaseError

    """


    def __init__(self, odkSettings, workingDirectory):

        self.odkID = odkSettings.odkID
        self.odkURL = odkSettings.odkURL
        self.odkUser = odkSettings.odkUser
        self.odkPassword = odkSettings.odkPassword
        self.odkFormID = odkSettings.odkFormID
        self.odkLastRun = odkSettings.odkLastRun
        self.odkLastRunDate = odkSettings.odkLastRunDate
        self.odkLastRunDatePrev = odkSettings.odkLastRunDatePrev
        # self.odkLastRunResult = odkSettings.odkLastRunResult
        bcDir = os.path.abspath(os.path.dirname(__file__))
        self.bcPath = os.path.join(bcDir, "libs/ODK-Briefcase-v1.10.1.jar")
        odkPath = os.path.join(workingDirectory, "ODKFiles")
        self.exportDir = odkPath
        self.storageDir = odkPath
        self.fileName = "odkBCExportNew.csv"

        try:
            if not os.path.isdir(odkPath):
                os.makedirs(odkPath)
        except:
            raise ODKError("Unable to create directory " + odkPath)

    def mergeToPrevExport(self):
        """Merge previous ODK Briefcase export files."""
        exportFile_prev = os.path.join(self.exportDir, "odkBCExportPrev.csv")
        exportFile_new = os.path.join(self.exportDir, self.fileName)

        isExportFile_prev = os.path.isfile(exportFile_prev)
        isExportFile_new = os.path.isfile(exportFile_new)

        if isExportFile_prev and isExportFile_new:
            with open(exportFile_new, "r", newline="") as fNew:
                fNewLines = fNew.readlines()
            with open(exportFile_prev, "r", newline="") as fPrev:
                fPrevLines = fPrev.readlines()
            with open(exportFile_prev, "a", newline="") as fCombined:
                for line in fNewLines:
                    if line not in fPrevLines:
                        fCombined.write(line)
            os.remove(exportFile_new)

        if isExportFile_new and not isExportFile_prev:
            shutil.move(exportFile_new, exportFile_prev)

    def briefcase(self):
        """Export records from ODK Aggregate server using ODK Briefcase.

        Longer description here.

        Returns
        -------
        Connection object
            SQL connection object for querying config settings

        Raises
        ------
        ODKError

        """


        bcArgs = ['java', '-jar', self.bcPath,
                  # '-e', '-oc', '-em', ## '-e' needed for ODK-Briefcase-v1.12.0.jar
                  '-oc', '-em',
                  '-id', str('"' + self.odkFormID    + '"'),
                  '-sd', str('"' + self.storageDir + '"'),
                  '-ed', str('"' + self.exportDir  + '"'),
                  '-f',  str('"' + self.fileName   + '"'),
                  '-url', self.odkURL,
                  '-u', self.odkUser,
                  '-p', self.odkPassword,
                  '-start', self.odkLastRunDatePrev]
        completed = subprocess.run(args = bcArgs,
                                   stdin  = subprocess.PIPE,
                                   stdout = subprocess.PIPE,
                                   stderr = subprocess.PIPE)
        if completed.returncode == 1:
            raise ODKError(completed.stderr)
        return(completed)

#------------------------------------------------------------------------------#
# Exceptions
#------------------------------------------------------------------------------#
class ODKError(PipelineError): pass