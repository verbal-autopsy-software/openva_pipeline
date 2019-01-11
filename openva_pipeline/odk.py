"""
openva_pipeline.odk
-------------------

This module uses ODK Briefcase to pull VA records from an ODK Aggregate server.
"""

import subprocess
import os
import shutil
from pysqlcipher3 import dbapi2 as sqlcipher

from .exceptions import PipelineError
from .exceptions import ODKError

class ODK:
    """Manages Pipeline's interaction with ODK Aggregate.

    This class handles the segment of the pipeline related to ODK.  The
    ODK.connect() method calls ODK Briefcase to connect with an ODK Aggregate
    server and export VA records.  It also checks for previously exported files
    and updates them as needed.  Finally, it logs messages and errors to the
    pipeline database.

    :param odkSettings: A named tuple with all of configuration settings as
      attributes.
    :type odkSettings: named tuple
    :param workingDirectory: Directory where openVA Pipeline should create
      files.
    :type workingDirectory: string
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
        # bcDir = os.path.abspath(os.path.dirname(__file__))
        # self.bcPath = os.path.join(bcDir, "libs/ODK-Briefcase-v1.12.2.jar")
        self.bcPath = os.path.join(workingDirectory, "ODK-Briefcase-v1.12.2.jar")
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
        """Calls ODK Briefcase.

        This method spawns a new process that runs the ODK Briefcase Java
        application (via a command-line interface) to download a CSV file
        with verbal autopsy records from an ODK Aggregate server.

        :returns: Return value from method subprocess.run()
        :rtype: subprocess.CompletedProcess
        :raises: ODKError
        """

        bcArgs = ['java', '-jar', self.bcPath,
                  '-url', str('"' + self.odkURL + '"'),
                  '-u', str('"' + self.odkUser + '"'),
                  '-p', str('"' + self.odkPassword + '"'),
                  '-id', str('"' + self.odkFormID + '"'),
                  ' -e ',
                  '-sd', str(self.storageDir),
                  '-ed', str(self.exportDir),
                  '-f',  str(self.fileName),
                  '-start', str('"' + self.odkLastRunDatePrev + '"'),
                  '-oc', '-em']

        completed = subprocess.run(args = bcArgs,
                                   stdin  = subprocess.PIPE,
                                   stdout = subprocess.PIPE,
                                   stderr = subprocess.PIPE)
        if completed.returncode == 1:
            raise ODKError(completed.stderr)
        return(completed)
