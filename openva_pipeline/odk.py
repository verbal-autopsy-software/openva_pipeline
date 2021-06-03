"""
openva_pipeline.odk
-------------------

This module uses ODK Briefcase to pull VA records from an ODK Aggregate server.
"""

import subprocess
import os
import shutil
import requests
import csv
import sys
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
        self.odkProjectNumber = odkSettings.odkProjectNumber
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

    def central(self):
        """Connects to ODK Central through api.

        This method calls requests.get to download a CSV file with verbal
        autopsy records from an ODK Collect server.

        :returns: Returns a string indicating the number of downloaded records.
        :rtype: string
        :raises: ODKError
        """
        exportFile_new = os.path.join(self.exportDir, self.fileName)
        url = os.path.join(self.odkURL, "v1/projects", self.odkProjectNumber,
                           "forms", self.odkFormID, "submissions.csv")
        username = self.odkUser
        password = self.odkPassword
        try:
            r = requests.get(url, auth=(username, password))
        except requests.exceptions.SSLError as e:
            raise ODKError(
                "Unable to connect to ODK Central (using requests): {0}".format(e))
        except:
            raise ODKError(
                "Unable to connect to ODK Central (unexpected error): {0}".format(sys.exc_info()))

        if r.status_code != 200:
            raise ODKError("Error getting data from ODK Central: {0}".format(r.text))

        odk_text = r.text.splitlines()
        n_records = len(odk_text) - 1
        odk_data = [i.split(",") for i in odk_text]
        with open(exportFile_new, "w") as f:
            writer = csv.writer(f)
            writer.writerows(odk_data)
        return("SUCCESS! Downloaded {} records".format(n_records))
