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
from pipeline import PipelineError
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
    exportDir : Directory where CSV file (containing VA records) is saved (ODK
      Briefcase parameter).
    storageDir : Directory where ODK Briefcase files are stored (ODK
      Briefcase parameter).
    fileName : Name of CSV file containing VA records exported from ODK
      Aggregate (ODK Briefcase parameter).

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


    def __init__(self, odkSettings, exportDir, storageDir):

        self.odkID = odkSettings.odkID
        self.odkURL = odkSettings.odkURL
        self.odkUser = odkSettings.odkUser
        self.odkPassword = odkSettings.odkPassword
        self.odkFormID = odkSettings.odkFormID
        self.odkLastRun = odkSettings.odkLastRun
        self.odkLastRunDate = odkSettings.odkLastRunDate
        self.odkLastRunDatePrev = odkSettings.odkLastRunDatePrev
        self.odkLastRunResult = odkSettings.odkLastRunResult
        self.bcPath = "libs/ODK-Briefcase-v1.10.1.jar" # need to specify module path
        # self.bcPath = "libs/ODK-Briefcase-v1.12.0.jar" # need to specify module path
        self.exportDir = exportDir # note: changing this (from one run to next)
        # will cause problems
        self.storageDir = storageDir
        self.fileName = os.path.join(self.exportDir, "odkBCExportNew.csv")

    def mergePrevExport(self):
        """Merge previous ODK Briefcase export files."""
        exportFile_Prev = os.path.join(self.exportDir, "odkBCExportPrev.csv")
        exportFile_New = self.fileName

        isExportFile_Prev = os.path.isfile(exportFile_Prev)
        isExportFile_New = os.path.isfile(exportFile_New)

        if isExportFile_Prev and isExportFile_New:
            with open(exportFile_New, "r", newline="") as fNew:
                fNewLines = fNew.readlines()
            with open(exportFile_Prev, "r", newline="") as fPrev:
                fPrevLines = fPrev.readlines()
            with open(exportFile_Prev, "a", newline="") as fCombined:
                for line in fNewLines:
                    if line not in fPrevLines:
                        fCombined.write(line)
            os.remove(exportFile_New)

        if isExportFile_New and not isExportFile_Prev:
            shutil.move(exportFile_New, exportFile_Prev)

    def briefcase(self):
        """Export records from ODK Aggregate server using ODK Briefcase.

        Longer description here.

        Returns
        -------
        Connection object
            SQL connection object for querying config settings

        Raises
        ------
        ODKBriefcaseError

        """

        ## check that briefcase is present
        ## does it need to be executable?

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
            raise ODKBriefcaseError(completed.stderr)

        return(completed)

#     def prepareExport():

# # make a copy of current ODK Briefcase Export file, to compare with new file once exported (if there is an existing export file)


#     # check if previous file exists from above operations and create delta file of new entries
#     if os.path.isfile(odkBCExportPrevious) == True:
#         try:
#             ## WARNING: odkBCExportPrevious & odkBCExportNewFil (CSV files)
#             ##          contain sensitive VA information (leaving them in folder)
#             with open(odkBCExportPrevious, "r", newline="") as t1, open(odkBCExportNewFile, "r", newline="") as t2:
#                 fileone = t1.readlines()
#                 filetwo = t2.readlines()
#                 header = filetwo[0]
#             with open(openVAReadyFile, "w", newline="") as outFile:
#                 outFile.write(header)
#                 for line in filetwo:
#                     if line not in fileone:
#                         outFile.write(line)
#         except OSError as e:
#             try:
#                 sql = "INSERT INTO EventLog (eventDesc, eventType, eventTime) VALUES"
#                 par = ("Could not create: " + openVAReadyFile, "Error", timeFMT)
#                 cursor.execute(sql, par)
#                 db.commit()
#             except (sqlcipher.Error, sqlcipher.Warning, sqlcipher.DatabaseError) as e:
#                 db.rollback()
#             errorMsg = [timeFMT, str(e), "Error: Could not create: " + openVAReadyFile]
#             cleanup(errorMsg)
#     else:
#         # if there is no pre-existing ODK Briefcase Export file, then copy and rename to OpenVAReadyFile.csv
#         try:
#             shutil.copy(odkBCExportNewFile, openVAReadyFile)
#         except (OSError, shutil.Error) as e:
#             try:
#                 sql = "INSERT INTO EventLog (eventDesc, eventType, eventTime) VALUES (?, ?, ?)"
#                 par = (e, "Error", timeFMT)
#                 cursor.execute(sql, par)
#                 db.commit()
#             except (sqlcipher.Error, sqlcipher.Warning, sqlcipher.DatabaseError) as e:
#                 db.rollback()
#             errorMsg = [timeFMT, str(e), "Error: Could not copy: " + odkBCExportNewFile + " to: " + openVAReadyFile]
#             cleanup(errorMsg)
        


#------------------------------------------------------------------------------#
# Exceptions
#------------------------------------------------------------------------------#
class ODKBriefcaseError(PipelineError): pass
