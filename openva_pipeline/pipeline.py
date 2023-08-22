"""
openva_pipeline.pipeline
------------------------

This module defines the primary API for the openVA Pipeline.
"""

import os
import csv
import datetime
import sys
from pandas import read_csv
from typing import Union, Dict

from .transfer_db import TransferDB
from .transfer_db import DatabaseConnectionError
from .odk import ODK
from .openva import OpenVA
from .dhis import DHIS
from .exceptions import PipelineError


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
    :type db_file_name: str
    :param db_directory: Path of folder containing the Transfer database.
    :type db_directory: str
    :param db_key: Encryption key for the Transfer database.
    :type db_key: str
    :param use_dhis: Indicator for telling pipeline to post records/tracked
    entity instances to DHIS
    :type use_dhis: bool
    """

    def __init__(self,
                 db_file_name: str,
                 db_directory: str,
                 db_key: str,
                 use_dhis: bool = True):

        self.db_file_name = db_file_name
        self.db_directory = db_directory
        self.db_key = db_key
        self.db_path = os.path.join(db_directory, db_file_name)
        now_date = datetime.datetime.now()
        self.pipeline_run_date = now_date.strftime("%Y-%m-%d_%H:%M:%S")
        self.use_dhis = use_dhis
        self.xfer_db = TransferDB(
            db_file_name=self.db_file_name,
            db_directory=self.db_directory,
            db_key=self.db_key,
            pl_run_date=self.pipeline_run_date,
        )
        self.settings = None
        self._config()
        self.dhis = None
        self.no_org_units = None

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
            par = (event_desc, event_type, time_fmt)
            self.xfer_db.insert_event_log(par)
        except DatabaseConnectionError as e:
            error_msg = [time_fmt, str(e), "Committed by Pipeline.log_event"]
            try:
                with open(error_file, "a", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(error_msg)
            except (PermissionError, OSError) as exc:
                print("Can't write to db_error_log.csv")
                print(error_msg.append(str(exc)))

    def _config(self) -> None:
        """Fetch configuration settings from Transfer DB.

        This method queries the Transfer database (DB) and returns objects that
        can be used as the arguments for other methods in this class, i.e.,
        :meth:`Pipeline.run_odk() <run_odk>`,
        :meth:`Pipeline.run_openva() <run_openva>`, and
        :meth:`Pipeline.run_dhis() <run_dhis>`.
        """

        settings_pipeline = self.xfer_db.config_pipeline()
        settings_odk = self.xfer_db.config_odk()
        settings_openva = self.xfer_db.config_openva(settings_pipeline.algorithm)
        settings = {
            "pipeline": settings_pipeline,
            "odk": settings_odk,
            "openva": settings_openva,
        }
        self.settings = settings
        if self.use_dhis:
            settings_dhis = self.xfer_db.config_dhis(settings_pipeline.algorithm)
            settings["dhis"] = settings_dhis

    def update_db(self):
        """Update transfer database created by previous version of the
        pipeline."""

        table_names = self._get_tables()
        conn = self.xfer_db.connect_db()
        c = conn.cursor()
        if "VA_Org_Unit_Not_Found" not in table_names:
            sql_make_table = (
                "CREATE TABLE VA_Org_Unit_Not_Found "
                "(id char(100) NOT NULL, "
                "outcome char(100), "
                "eventBlob blob, "
                "evaBlob blob, "
                "dhisOrgUnit char(500), "
                "dateEntered date,"
                "fixed char(5));"
            )
            c.execute(sql_make_table)

        dhis_table = self._get_fields("DHIS_Conf")
        dhis_fields = [entry[0] for entry in dhis_table]
        if "dhisPostRoot" not in dhis_fields:
            sql_make_field = "ALTER TABLE DHIS_Conf ADD dhisPostRoot char(5);"
            c.execute(sql_make_field)
            sql_fill_field = "UPDATE DHIS_Conf SET dhisPostRoot = 'False';"
            c.execute(sql_fill_field)

        odk_table = self._get_fields("ODK_Conf")
        odk_fields = [entry[0] for entry in odk_table]
        if "odkUseCentral" not in odk_fields:
            sql_make_field = "ALTER TABLE ODK_Conf ADD odkUseCentral char(5);"
            c.execute(sql_make_field)
            sql_fill_field = "UPDATE ODK_Conf SET odkUseCentral = 'False';"
            c.execute(sql_make_field)
        if "odkProjectNumber" not in odk_fields:
            sql_make_field = "ALTER TABLE ODK_Conf ADD odkProjectNumber char(6);"
            c.execute(sql_make_field)
        conn.close()

    def _update_odk(self, field, value):
        """Update ODK_Conf.field(s) with value(s) in Transfer DB and
        instance settings attribute."""
        self.xfer_db.update_table("ODK_Conf", field, value)
        self._config()

    def _update_dhis(self, field, value):
        """Update dhis_Conf.field(s) with value(s) in Transfer DB and
        instance settings attribute."""
        self.xfer_db.update_table("DHIS_Conf", field, value)
        self._config()

    def _update_pipeline(self, field, value):
        """Update pipeline_Conf.field(s) with value(s) in Transfer DB and
        instance settings attribute."""
        self.xfer_db.update_table("Pipeline_Conf", field, value)
        self._config()

    def _get_odk_conf(self):
        """Get values from ODK_Conf table in Transfer DB."""
        odk_conf_values = self.xfer_db.get_table_conf("ODK_Conf")
        return odk_conf_values

    def _get_dhis_conf(self):
        """Get values from dhis_Conf table in Transfer DB."""
        dhis_conf_values = self.xfer_db.get_table_conf("DHIS_Conf")
        return dhis_conf_values

    def _get_pipeline_conf(self):
        """Get values from pipeline_Conf table in Transfer DB."""
        pipeline_conf_values = self.xfer_db.get_table_conf("Pipeline_Conf")
        return pipeline_conf_values

    def _get_tables(self):
        """Get table names from Transfer DB."""
        tables = self.xfer_db.get_tables()
        return tables

    def _get_fields(self, table):
        """Get field names from table in Transfer DB."""
        fields = self.xfer_db.get_fields(table)
        return fields

    def _get_schema(self, table):
        """Get schema of table in Transfer DB."""
        schema = self.xfer_db.get_schema(table)
        return schema

    def _get_log(self, n: int = 5, last: bool = True):
        """Get n rows from in EventLog table in Transfer DB."""
        log_messages = self.xfer_db.get_event_log(n_messages=n,
                                                  recent=last)
        return log_messages

    def run_odk(self):
        """Run check duplicates, copy file, and briefcase.

        This method downloads data from either (1) an ODK Central server,
        using :meth:`ODK.central() <openva_pipeline.odk.ODK.central>`, or
        (2) an ODK Aggregate server using the Java application ODK Briefcase,
        by calling the method
        :meth:`ODK.briefcase() <openva_pipeline.odk.ODK.briefcase>`.  The
        configuration settings are taken from the argument
        argsODK (see :meth:`Pipeline.config() <config>`),
        and downloads verbal autopsy (VA)
        records as a (csv) export from an ODK Central/Aggregate server.
        If there is a previous ODK export file, this method merges the files by
        keeping only the unique VA records.

        :return: Summary of results from ODK step
        :rtype: tuple
        """

        args_odk = self.settings["odk"]
        pipeline_odk = ODK(self.settings)
        pipeline_odk.merge_to_prev_export()
        if args_odk.odk_use_central == "True":
            odk_central = pipeline_odk.central()
        else:
            odk_bc = pipeline_odk.briefcase()
        self.xfer_db.config_pipeline()
        odk_summary = self.xfer_db.check_duplicates(self.use_dhis)
        if args_odk.odk_use_central == "True":
            return odk_central, odk_summary
        else:
            return odk_bc, odk_summary

    def run_openva(self):
        """Create & run script or run smartva.

        This method runs the through the suite of methods in the
        :class:`OpenVA <openva_pipeline.openVA.OpenVA>`.
        class.  The list of tasks performed (in order) are: (1) call the method
        :meth:`OpenVA.prep_va_data() <openva_pipeline.openVA.OpenVA.prep_va_data>`
        to copy over CSV files with VA data (retrieved from ODK Aggregate);
        (2) use the method
        :meth:`OpenVA.r_script() <openva_pipeline.openVA.OpenVA.r_script>`
        to create an R script; and (3) call the method
        :meth:`OpenVA.get_cod() <openva_pipeline.openVA.OpenVA.get_cod>` to
        run the R script that estimates the causes of death and stores the
        results in "OpenVAFiles/recordStorage.csv" and
        "OpenVAFiles/entityAttributeValue.csv" (the former serving as the
        blob posted to DHIS2).

        :return: an indicator of zero VA records in the ODK export
        :rtype: dictionary
        """

        pipeline_openva = OpenVA(
            settings=self.settings,
            pipeline_run_date=self.pipeline_run_date,
        )
        r_out = pipeline_openva.prep_va_data()
        if r_out["n_to_openva"] > 0:
            pipeline_openva.r_script()
            completed = pipeline_openva.get_cod()
            # r_out["completed"] = completed
            r_out["return_code"] = completed.returncode
            summary = pipeline_openva.get_summary()
            r_out.update(summary)
        else:
            r_out["n_processed"] = 0
            r_out["n_cod_missing"] = 0
        return r_out

    def _connect_dhis(self):
        """Create connection to DHIS API. """

        args_dhis = self.settings['dhis']
        args_pipeline = self.settings['pipeline']
        self.dhis = DHIS(args_dhis, args_pipeline.working_directory)

    def _check_use_dhis(self) -> None:
        """Check if Pipeline is set to use DHIS and, if so, check connection
        and create if necessary"""

        if self.use_dhis is False:
            raise PipelineError("This Pipeline instance was created with the "
                                "argument use_dhis=False (so cannot access "
                                "configuration settings for DHIS used by "
                                "Pipeline.run_dhis)")
        elif self.dhis is None:
            self._connect_dhis()

    def run_dhis(self) -> Dict[str, str]:
        """Connect to API and post events.

        This method first calls the method
        :meth:`DHIS.connect() <openva_pipeline.dhis.DHIS.connect>`
        to establish a connection with a DHIS2 server and, second
        calls the method
        :meth:`DHIS.post_va() <openva_pipeline.dhis.DHIS.post_va>` to
        post VA data, the assigned causes of death, and associated
        metadata (concerning cause assignment).

        :return: VA Program ID from the DHIS2 server, the log from
          the DHIS2 connection, and the number of records posted to DHIS2
        :rtype: dictionary
        """

        self._check_use_dhis()
        post_log = self.dhis.post_va(self.xfer_db)
        if self.dhis.post_to_tracker:
            self.dhis.verify_tei_post(post_log)
        else:
            self.dhis.verify_post(post_log)

        dhis_out = {
            "va_program_uid": self.dhis.va_program_uid,
            "post_log": post_log,
            "n_posted_events": self.dhis.n_posted_events,
            "n_no_valid_org_unit": self.dhis.n_no_valid_org_unit,
        }
        return dhis_out

    def get_no_org_unit(self,
                        va_id: str = None) -> Dict:
        """Get VA record IDs that do not have a valid organisation unit
        for posting to DHIS2; or, if va_id is provided, get the eventBlob and
        evaBlob for that VA.

        :param va_id: VA's ID for which the eventBlob and evaBlob will be
        returned.
        :type va_id: str

        :return: VA record ID and data used to find DHIS2 org unit; or, if
        va_id is provided, the eventBlob and evaBlob for that VA.
        :rtype: dictionary
        """

        self._check_use_dhis()
        return self.xfer_db.get_no_ou_va(va_id)

    def fix_no_org_unit(self,
                        va_id: str,
                        org_unit: str) -> Dict:
        """Post a VA event to DHIS2 with provided organisation unit.  If the
        post is successful, the corresponding record will be removed from
        the Transfer database table VA_Org_Unit_Not_Found and added to the
        VA_Storage table.

        :param va_id: ID for VA record to post
        :type va_id: str
        :param org_unit: New DHIS2 organisation unit (display name or ID) where
        VA event will be posted.
        :type org_unit: str

        :return: Message indicating successful or unsuccessful post.
        :rtype: str
        """

        org_unit_id = self._check_fix_no_ou(va_id, org_unit)
        if org_unit_id == "fail":
            raise PipelineError("Post to DHIS2 was unsuccessful "
                                "(invalid va_id or org_unit)")
        va_data = self.xfer_db.get_no_ou_va(va_id)
        log = self.dhis.post_single_va(va_dict=va_data["va_dict"],
                                       eav=va_data["eav_dataframe"],
                                       org_unit=org_unit_id)
        if self.dhis.post_to_tracker:
            parsed_log = self.dhis._parse_tei_post_log(log=log)
        else:
            parsed_log = self.dhis._parse_post_log(log=log)
        log_summary = list(parsed_log.values())[0]
        verify_post = self.dhis.verify_single_va(va_id=va_id,
                                                 post_log=log)
        if verify_post:
            self.xfer_db.remove_no_ou_va(va_id=va_id)
            self.xfer_db.store_single_va(va_dict=va_data["va_dict"],
                                         org_unit_id=org_unit_id,
                                         log_summary=log_summary,
                                         dhis_tracker=self.dhis.post_to_tracker)
            return log_summary
        else:
            print("Unable to verify the post to DHIS2 was successful.")
            return log

    def _check_fix_no_ou(self, va_id: str, org_unit: str) -> str:
        """Check that va_id is in the Transfer database table
        VA_Org_Unit_Not_Found and that the org_unit is valid.

        :param va_id: ID for VA record to post
        :type va_id: str
        :param org_unit: New DHIS2 organisation unit (display name or ID) where
        VA event will be posted.
        :type org_unit: str

        :return: DHIS2 organisation unit ID or fail
        :rtype: str
        """

        self._check_use_dhis()
        va_no_org_unit = self.xfer_db.get_no_ou_va()
        if va_id not in va_no_org_unit.keys():
            print(f"{va_id} is not in Transfer database table "
                  "VA_Org_Unit_Not_Found")
            return "fail"
        va_storage_ids = self.xfer_db._get_va_storage_ids()
        if va_id in va_storage_ids:
            print(f"{va_id} is already stored in VA_Storage table!")
            return "fail"
        valid_org_units = self.get_dhis_org_units()
        if org_unit in valid_org_units.keys():
            return valid_org_units[org_unit]
        elif org_unit in valid_org_units.values():
            return org_unit
        else:
            print(f"{org_unit} is not a valid DHIS2 organisation unit")
            return "fail"

    def get_dhis_org_units(self, va_program: bool = True) -> Dict:
        """Get DHIS organisation unit IDs and display names.

        :param va_program: Indicator for returning only organisation units
        in the DHIS VA Program (as opposed to all organisation units).
        :type va_program: bool
        :returns: displayName: id of DHIS organisation units.
        :rtype: dict
        """

        self._check_use_dhis()
        return self.dhis._get_org_units(va_program=va_program)

    def store_results_db(self):
        """Store VA results in Transfer database."""

        self.xfer_db.config_pipeline()
        if not self.use_dhis:
            args_pipeline = self.settings["pipeline"]
            working_directory = args_pipeline.working_directory
            record_storage_path = os.path.join(
                working_directory,
                "OpenVAFiles/record_storage.csv")
            new_storage_path = os.path.join(
                working_directory,
                "OpenVAFiles/new_storage.csv")
            record_storage = read_csv(record_storage_path)
            record_storage["pipelineOutcome"] = "Assigned a cause of death"
            missing_cod = record_storage["cod"] == "MISSING"
            record_storage.loc[missing_cod,
                               "pipelineOutcome"] = "No cause assigned"
            record_storage.to_csv(new_storage_path)
        self.xfer_db.store_va(self.dhis.post_to_tracker)

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

        self.xfer_db.config_pipeline()
        self.xfer_db.clean_odk()
        self.xfer_db.clean_openva()
        if self.use_dhis:
            self.xfer_db.clean_dhis()
        self.xfer_db.update_odk_last_run()
