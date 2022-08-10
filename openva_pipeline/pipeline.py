"""
openva_pipeline.pipeline
------------------------

This module defines the primary API for the openVA Pipeline.
"""

import os
import csv
import datetime
import sys

from .transfer_db import TransferDB
from .transfer_db import DatabaseConnectionError
from .odk import ODK
from .openva import OpenVA
from .dhis import DHIS


class Pipeline:
    """Primary API for the openVA pipeline.

    This class calls three others to move verbal autopsy data from an ODK
    Aggregate server (using the ODK class), through the openVA R package to
    assign cause of death (using the OpenVA class), and deposits the VA records
    with assigned causes to either/both a DHIS server (using the DHIS class) or
    the Transfer database -- a local database which also contains configuration
    settings for the pipeline.  The TransferDB class performs the final step of
    storing the results locally as well as accessing the configuration
    settings.

    :param db_file_name: File name of the Transfer database.
    :type db_file_name: string
    :param db_directory: str
        Path of folder containing the Transfer database.
    :type db_directory: string
    :param db_key: Encryption key for the Transfer database.
    :type db_key: string
    """

    def __init__(self, db_file_name, db_directory, db_key, use_dhis=True):

        self.db_file_name = db_file_name
        self.db_directory = db_directory
        self.db_key = db_key
        self.db_path = os.path.join(db_directory, db_file_name)
        now_date = datetime.datetime.now()
        self.pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        self.use_dhis = use_dhis

    def log_event(self, event_desc, event_type):
        """Commit event or error message into EventLog table of transfer
        database.

        :param event_desc: Description of the event.
        :type event_desc: string
        :param event_type: Type of event (error or information)
        :type event_type: string
        """

        error_file = os.path.join(self.db_directory, "db_error_log.csv")
        time_fmt = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        if not os.path.isfile(error_file):
            try:
                with open(error_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        ["Date"] + ["Description"] + ["Additional Information"]
                    )
            except OSError as e:
                print(str(e) + "...Can't create db_error_log.csv")
                sys.exit(1)
        try:
            xfer_db = TransferDB(
                db_file_name=self.db_file_name,
                db_directory=self.db_directory,
                db_key=self.db_key,
                pl_run_date=self.pipeline_run_date,
            )
            conn = xfer_db.connect_db()
            c = conn.cursor()
            sql = "INSERT INTO EventLog \
                   (eventDesc, eventType, eventTime) VALUES (?, ?, ?)"
            par = (event_desc, event_type, time_fmt)
            c.execute(sql, par)
            conn.commit()
            conn.close()
        except DatabaseConnectionError as e:
            error_msg = [time_fmt, str(e), "Committed by Pipeline.log_event"]
            try:
                with open(error_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(error_msg)
            except (PermissionError, OSError) as exc:
                print("Can't write to db_error_log.csv")
                print(error_msg.append(str(exc)))

    def config(self):
        """Fetch configuration settings from Transfer DB.

        This method queries the Transfer database (DB) and returns objects that
        can be used as the arguments for other methods in this class, i.e.,
        :meth:`Pipeline.run_odk() <run_odk>`,
        :meth:`Pipeline.run_openva() <run_openva>`, and
        :meth:`Pipeline.run_dhis() <run_dhis>`.
        :returns: Configuration settings for pipeline steps (e.g. connecting
          to ODK Aggregate, running openVA, or posting records to DHIS)
        :rtype: dictionary
        """

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        settings_pipeline = xfer_db.config_pipeline(conn)
        settings_odk = xfer_db.config_odk(conn)
        settings_openva = xfer_db.config_openva(
            conn, settings_pipeline.algorithm)
        settings = {
            "pipeline": settings_pipeline,
            "odk": settings_odk,
            "openva": settings_openva,
        }
        if self.use_dhis:
            settings_dhis = xfer_db.config_dhis(conn,
                                                settings_pipeline.algorithm)
            settings["dhis"] = settings_dhis
        conn.close()
        return settings

    def _update_odk(self, field, value):
        """Update ODK_Conf.field(s) with value(s) in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        xfer_db.update_odk_conf(conn, field, value)
        conn.close()

    def _update_dhis(self, field, value):
        """Update dhis_Conf.field(s) with value(s) in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        xfer_db.update_dhis_conf(conn, field, value)
        conn.close()

    def _update_pipeline(self, field, value):
        """Update pipeline_Conf.field(s) with value(s) in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        xfer_db.update_pipeline_conf(conn, field, value)
        conn.close()

    def _get_odk_conf(self):
        """Get values from ODK_Conf table in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        odk_conf_values = xfer_db.get_odk_conf(conn)
        conn.close()
        return odk_conf_values

    def _get_dhis_conf(self):
        """Get values from dhis_Conf table in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        dhis_conf_values = xfer_db.get_dhis_conf(conn)
        conn.close()
        return dhis_conf_values

    def _get_pipeline_conf(self):
        """Get values from pipeline_Conf table in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        pipeline_conf_values = xfer_db.get_pipeline_conf(conn)
        conn.close()
        return pipeline_conf_values

    def _get_tables(self):
        """Get table names from Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        tables = xfer_db.get_tables(conn)
        conn.close()
        return tables

    def _get_fields(self, table):
        """Get field names from table in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        fields = xfer_db.get_fields(conn, table)
        conn.close()
        return fields

    def _get_schema(self, table):
        """Get schema of table in Transfer DB."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        schema = xfer_db.get_schema(conn, table)
        conn.close()
        return schema

    def run_odk(self, settings):
        """Run check duplicates, copy file, and briefcase.

        This method downloads data from either (1) an ODK Central server,
        using :meth:`ODK.central() <openva_pipeline.odk.ODK.central>`, or
        (2) an ODK Aggregate server using the Java application ODK Briefcase,
        by calling the method
        :meth:`ODK.briefcase() <openva_pipeline.odk.ODK.briefcase>`.  The
        configuration settings are taken from the argument
        argsODK (see :meth:`Pipeline.config() <config>`)
        , and downloads verbal autopsy (VA)
        records as a (csv) export from an ODK Central/Aggregate server.
        If there is a previous ODK export file, this method merges the files by
        keeping only the unique VA records.

        :param settings: Configuration settings for pipeline steps (which is
        returned from :meth:`Pipeline.config() <config>`).
        :type settings: dictionary of named tuples
        :return: Return value from method subprocess.run()
        :rtype: subprocess.CompletedProcess
        """

        args_odk = settings['odk']
        pipeline_odk = ODK(settings)
        pipeline_odk.merge_to_prev_export()
        if args_odk.odk_use_central == "True":
            odk_central = pipeline_odk.central()
        else:
            odk_bc = pipeline_odk.briefcase()
        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        xfer_db.config_pipeline(conn)
        xfer_db.check_duplicates(conn)
        conn.close()
        if args_odk.odk_use_central == "True":
            return odk_central
        else:
            return odk_bc

    def run_openva(self, settings):
        """Create & run script or run smartva.

        This method runs the through the suite of methods in the
        :class:`OpenVA <openva_pipeline.openVA.OpenVA>`.
        class.  The list of tasks performed (in order) are: (1) call the method
        :meth:`OpenVA.copy_va() <openva_pipeline.openVA.OpenVA.copy_va>`
        to copy over CSV files with VA data (retrieved from ODK Aggregate);
        (2) use the method
        :meth:`OpenVA.r_script() <openva_pipeline.openVA.OpenVA.r_script>`
        to create an R script; and (3) call the method
        :meth:`OpenVA.get_cod() <openva_pipeline.openVA.OpenVA.get_cod>` to
        run the R script that estimates the causes of death and stores the
        results in "OpenVAFiles/recordStorage.csv" and
        "OpenVAFiles/entityAttributeValue.csv" (the former serving as the
        blob posted to DHIS2).

        :param settings: Configuration settings for pipeline steps (which is
        returned from :meth:`Pipeline.config() <config>`).
        :type settings: dictionary of named tuples
        :return: an indicator of zero VA records in the ODK export
        :rtype: dictionary
        """

        pipeline_openva = OpenVA(
            settings=settings,
            pipeline_run_date=self.pipeline_run_date,
        )
        zero_records = pipeline_openva.copy_va()
        r_out = {"zero_records": zero_records}
        if not zero_records:
            pipeline_openva.r_script()
            completed = pipeline_openva.get_cod()
            # r_out["completed"] = completed
            r_out["return_code"] = completed.returncode
        return r_out

    def run_dhis(self, settings):
        """Connect to API and post events.

        This method first calls the method
        :meth:`DHIS.connect() <openva_pipeline.dhis.DHIS.connect>`
        to establish a connection with a DHIS2 server and, second
        calls the method
        :meth:`DHIS.post_va() <openva_pipeline.dhis.DHIS.post_va>` to
        post VA data, the assigned causes of death, and associated
        metadata (concerning cause assignment).

        :param settings: Configuration settings for pipeline steps (which is
        returned from :meth:`Pipeline.config() <config>`).
        :type settings: dictionary of named tuples
        :return: VA Program ID from the DHIS2 server, the log from
          the DHIS2 connection, and the number of records posted to DHIS2
        :rtype: dictionary
        """

        args_dhis = settings['dhis']
        args_pipeline = settings['pipeline']
        pipeline_dhis = DHIS(args_dhis, args_pipeline.working_directory)
        api_dhis = pipeline_dhis.connect()
        post_log = pipeline_dhis.post_va(api_dhis)
        pipeline_dhis.verify_post(post_log, api_dhis)

        dhis_out = {
            "va_program_uid": pipeline_dhis.va_program_uid,
            "post_log": post_log,
            "n_posted_events": pipeline_dhis.n_posted_events,
        }
        return dhis_out

    def store_results_db(self):
        """Store VA results in Transfer database."""

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        xfer_db.config_pipeline(conn)
        xfer_db.store_va(conn)
        conn.close()

    def close_pipeline(self):
        """Update ODK_Conf ODKLastRun in Transfer DB and clean up files.

        This method calls methods in the
        :class:`TransferDB <openva_pipeline.transferDB.TransferDB>`
        class to remove the data files created at each step of the
        pipeline.  More specifically, it runs
        :meth:`TransferDB.clean_odk()
        <openva_pipeline.transferDB.TransferDB.clean_odk>`
        to remove the ODK Briefcase export files ("ODKFiles/odkBCExportNew.csv"
        and "ODKFiles/odkBCExportPrev.csv") if they exist;
        :meth:`TransferDB.clean_openva()
        <openva_pipeline.transferDB.TransferDB.clean_openva>`
        to remove the input data file ("OpenVAFiles/openva_input.csv") and the
        output files ("OpenVAFiles/record_storage.csv",
        "OpenVAFiles/new_storage.csv", and
        "OpenVAFiles/entity_attribute_value.csv") -- note that all of these
        results are stored in either/both of the Transfer DB and the DHIS2
        server's VA program; and, third, the method
        :meth:`TransferDB.clean_dhis()
        <openva_pipeline.transferDB.TransferDB.clean_dhis>`
        is called to remove the blobs posted to the DHIS2 server and stored in
        the folder "DHIS/blobs".  Finally, this method updates the Transfer
        DB's value in the ODK_Conf table's variable odk_last_run so the next ODK
        Export file does not include VA records already processed through the
        pipeline.
        """

        xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        conn = xfer_db.connect_db()
        xfer_db.config_pipeline(conn)
        xfer_db.clean_odk()
        xfer_db.clean_openva()
        xfer_db.clean_dhis()
        xfer_db.update_odk_last_run(conn, self.pipeline_run_date)
        conn.close()
