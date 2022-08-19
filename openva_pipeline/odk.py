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

from .exceptions import ODKError


class ODK:
    """Manages Pipeline's interaction with ODK Aggregate.

    This class handles the segment of the pipeline related to ODK.  The
    ODK.connect() method calls ODK Briefcase to connect with an ODK Aggregate
    server and export VA records.  It also checks for previously exported files
    and updates them as needed.  Finally, it logs messages and errors to the
    pipeline database.

    :param settings: Configuration settings for pipeline steps (which is
    returned from :meth:`Pipeline.config() <config>`).
    :type settings: dictionary of named tuples
    """

    def __init__(self, settings):

        odk_settings = settings["odk"]
        pipeline_settings = settings["pipeline"]
        self.odk_id = odk_settings.odk_id
        self.odk_url = odk_settings.odk_url
        self.odk_user = odk_settings.odk_user
        self.odk_password = odk_settings.odk_password
        self.odk_form_id = odk_settings.odk_form_id
        self.odk_last_run = odk_settings.odk_last_run
        self.odk_last_run_date = odk_settings.odk_last_run_date
        self.odk_last_run_date_prev = odk_settings.odk_last_run_date_prev
        # self.odkLastRunResult = odk_settings.odkLastRunResult
        # bcDir = os.path.abspath(os.path.dirname(__file__))
        # self.bc_path = os.path.join(bcDir, "libs/ODK-Briefcase-v1.12.2.jar")
        self.bc_path = os.path.join(pipeline_settings.working_directory,
                                    "ODK-Briefcase-v1.18.0.jar")
        self.odk_project_number = odk_settings.odk_project_number
        odk_path = os.path.join(pipeline_settings.working_directory,
                                "ODKFiles")
        self.export_dir = odk_path
        self.storage_dir = odk_path
        self.file_name = "odk_export_new.csv"
        self.n_records = None

        try:
            if not os.path.isdir(odk_path):
                os.makedirs(odk_path)
        except (PermissionError, OSError) as exc:
            raise ODKError("Unable to create directory " + odk_path) from exc

    def merge_to_prev_export(self):
        """Merge previous ODK Briefcase export files."""

        export_file_prev = os.path.join(self.export_dir,
                                        "odk_export_prev.csv")
        export_file_new = os.path.join(self.export_dir, self.file_name)

        is_export_file_prev = os.path.isfile(export_file_prev)
        is_export_file_new = os.path.isfile(export_file_new)

        if is_export_file_prev and is_export_file_new:
            with open(export_file_new, "r", newline="") as f_new:
                f_new_lines = f_new.readlines()
            with open(export_file_prev, "r", newline="") as f_prev:
                f_prev_lines = f_prev.readlines()
            with open(export_file_prev, "a", newline="") as f_combined:
                for line in f_new_lines:
                    if line not in f_prev_lines:
                        f_combined.write(line)
            os.remove(export_file_new)

        if is_export_file_new and not is_export_file_prev:
            shutil.move(export_file_new, export_file_prev)

    def briefcase(self):
        """Calls ODK Briefcase.

        This method spawns a new process that runs the ODK Briefcase Java
        application (via a command-line interface) to download a CSV file
        with verbal autopsy records from an ODK Aggregate server.

        :returns: Return value from method subprocess.run()
        :rtype: subprocess.CompletedProcess
        :raises: ODKError
        """

        # bc_args_plla = ['java', '-jar', self.bc_path,
        #                '-plla',
        #                '--odk_url', str('"' + self.odk_url + '"'),
        #                '--odk_username', str('"' + self.odk_user + '"'),
        #                '--odk_password', str('"' + self.odk_password + '"'),
        #                '--storage_directory', str(self.storage_dir),
        #                '--form_id', str('"' + self.odk_form_id + '"'),
        #                '-e',
        #                '--export_directory', str(self.export_dir),
        #                '--export_filename',  str(self.file_name),
        #                '--export_start_date',
        #                str('"' + self.odk_last_run_date_prev + '"'),
        #                '--overwrite_csv_export', '--exclude_media_export']
        bc_args_plla = [
            "java",
            "-jar",
            self.bc_path,
            "-plla",
            "--odk_url",
            str('"' + self.odk_url + '"'),
            "--odk_username",
            str('"' + self.odk_user + '"'),
            "--odk_password",
            str('"' + self.odk_password + '"'),
            "--storage_directory",
            str(self.storage_dir),
            "--form_id",
            str('"' + self.odk_form_id + '"'),
        ]
        try:
            subprocess.run(
                args=bc_args_plla,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise ODKError(str(exc.stderr)) from exc
        bc_args_export = [
            "java",
            "-jar",
            self.bc_path,
            "-e",
            "--form_id",
            str('"' + self.odk_form_id + '"'),
            "--storage_directory",
            str(self.storage_dir),
            "--export_directory",
            str(self.export_dir),
            "--export_filename",
            str(self.file_name),
            "--export_start_date",
            str('"' + self.odk_last_run_date_prev + '"'),
            "--overwrite_csv_export",
            "--exclude_media_export",
        ]
        try:
            completed_export = subprocess.run(
                args=bc_args_export,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            raise ODKError(str(exc.stderr)) from exc
        return completed_export

    def central(self):
        """Connects to ODK Central through api.

        This method calls requests.get to download a CSV file with verbal
        autopsy records from an ODK Collect server.

        :returns: Returns a string indicating the number of downloaded records.
        :rtype: string
        :raises: ODKError
        """
        export_file_new = os.path.join(self.export_dir, self.file_name)
        url = os.path.join(
            self.odk_url,
            "v1/projects",
            self.odk_project_number,
            "forms",
            self.odk_form_id,
            "submissions.csv",
        )
        data_filter = (
            "?$filter=__system/submissionDate%20ge%20"
            + self.odk_last_run_date.replace("/", "-")
        )
        username = self.odk_user
        password = self.odk_password
        try:
            r = requests.get(url + data_filter, auth=(username, password))
        except requests.exceptions.ConnectionError as e:
            raise ODKError(
                "Network problem, unable to connect to ODK Central" +
                " (using requests): {0}".format(e)
            )
        except requests.exceptions.Timeout as e:
            raise ODKError(
                "ODK Central server failing to respond after establishing" +
                " a connection (using requests): {0}".format(e)
            )
        except requests.exceptions.TooManyRedirects as e:
            raise ODKError(
                "Exceeded number of maximum redirections from ODK Central " +
                " server (using requests): {0}".format(e)
            )

        if r.status_code != 200:
            raise ODKError(
                "Error getting data from ODK Central: {0}".format(r.text))

        odk_text = r.text.splitlines()
        self.n_records = len(odk_text) - 1
        odk_reader = csv.reader(odk_text, delimiter=',', quotechar='"')
        odk_data = [row for row in odk_reader]
        with open(export_file_new, "w") as f:
            writer = csv.writer(f)
            writer.writerows(odk_data)
        return f"Downloaded {self.n_records} records."
