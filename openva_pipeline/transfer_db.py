"""
openva_pipeline.transfer_db
-----------------

This module handles interactions with the Transfer database.
"""

import os
from typing import Union, NamedTuple, List, Tuple, Dict
from shutil import rmtree
from collections import namedtuple
from datetime import datetime, timedelta
import sqlite3
from pickle import dumps
import json

from pandas import read_csv, DataFrame
from sqlcipher3 import dbapi2 as sqlcipher

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

    :param db_file_name: File name of the Transfer database.
    :type db_file_name: str
    :param db_directory: Path of folder containing the Transfer database.
    :type db_directory: str
    :param db_key: Encryption key for the Transfer database.
    :type db_key: str
    :param pl_run_date: Date when pipeline started latest
      run (YYYY-MM-DD_hh:mm:ss).
    :type pl_run_date: str
    """

    def __init__(self,
                 db_file_name: str,
                 db_directory: str,
                 db_key: str,
                 pl_run_date: str):

        self.db_file_name = db_file_name
        self.db_directory = db_directory
        self.db_key = db_key
        self.db_path = os.path.join(db_directory, db_file_name)
        self.working_directory = None
        self.pl_run_date = pl_run_date

    def _connect_db(self) -> sqlcipher.connect:
        """Connect to Transfer database.

        Uses parameters supplied to the parent class, TransferDB, to connect to
        the (encrypted) Transfer database.

        :returns: Used to query (encrypted) SQLite database.
        :rtype: SQLite database connection object
        :raises: DatabaseConnectionError
        """

        db_file_present = os.path.isfile(self.db_path)
        if not db_file_present:
            raise DatabaseConnectionError("")

        conn = sqlcipher.connect(self.db_path)
        par_set_key = '"' + self.db_key + '"'
        conn.execute("PRAGMA key = " + par_set_key)
        try:
            sql_test_connection = (
                "SELECT name FROM SQLITE_MASTER " "where type = 'table';"
            )
            conn.execute(sql_test_connection)
        except sqlcipher.DatabaseError as e:
            raise DatabaseConnectionError("Database password error..." +
                                          str(e))
        return conn

    def config_pipeline(self) -> NamedTuple:
        """Grabs Pipeline configuration settings.

        This method queries the Pipeline_Conf table in Transfer database and
        returns a tuple with attributes (1) algorithmMetadataCode; (2)
        codSource; (3) algorithm; and (4) working_directory.

        :returns: Arguments needed to configure the OpenVA Pipeline
          algorithmMetadataCode - attribute describing VA data
          codSource - attribute detailing the source of the Cause of Death list
          algorithm - attribute indicating which VA algorithm to use
          working_directory - attribute indicating the working directory
        :rtype: (named) tuple
        :raises: PipelineConfigurationError
        """

        conn = self._connect_db()
        c = conn.cursor()
        try:
            sql_pipeline = (
                "SELECT algorithmMetadataCode, codSource, algorithm, "
                "workingDirectory FROM Pipeline_Conf;"
            )
            query_pipeline = c.execute(sql_pipeline).fetchall()
            conn.close()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table Pipeline_Conf..." + str(e)
            )
        algorithm_metadata_code = query_pipeline[0][0]
        # if algorithm_metadata_code not in
        # [j for i in metadataQuery for j in i]:
        #     raise PipelineConfigurationError \
        #         ("Problem in database: Pipeline_Conf.algorithmMetadataCode")
        cod_source = query_pipeline[0][1]
        if cod_source not in ("ICD10", "WHO", "Tariff"):
            raise PipelineConfigurationError(
                "Problem in database: Pipeline_Conf.codSource"
            )
        algorithm = query_pipeline[0][2]
        if algorithm not in ("InterVA", "InSilicoVA", "SmartVA"):
            raise PipelineConfigurationError(
                "Problem in database: Pipeline_Conf.algorithm"
            )
        working_directory = query_pipeline[0][3]
        if not os.path.isdir(working_directory):
            raise PipelineConfigurationError(
                "Problem in database: Pipeline_Conf.workingDirectory"
            )

        nt_pipeline = namedtuple(
            "nt_pipeline",
            ["algorithm_metadata_code", "cod_source", "algorithm",
             "working_directory"],
        )
        settings_pipeline = nt_pipeline(
            algorithm_metadata_code, cod_source, algorithm, working_directory
        )
        self.working_directory = working_directory
        return settings_pipeline

    def config_odk(self) -> NamedTuple:
        """Query ODK configuration settings from database.

        This method is intended to be used in conjunction with (1)
        :meth:`TransferDB.connect_db() <connect_db>`, which establishes a
        connection to a database with the Pipeline configuration settings;
        and (2) :meth:`ODK.briefcase() <openva_pipeline.odk.ODK.briefcase>`,
        which establishes a connection to an ODK Aggregate server.  Thus,
        TransferDB.config_odk() gets its input from
        :meth:`TransferDB.connect_db() <connect_db>` and the output from
        TransferDB.config_odk() is a valid argument for
        :meth:`ODK.briefcase()<openva_pipeline.odk.ODK.briefcase>`.

        :returns: Contains all parameters for
          :meth:`ODK.briefcase()<openva_pipeline.odk.ODK.briefcase>`.
        :rtype: (named) tuple
        :raises: ODKConfigurationError
        """

        conn = self._connect_db()
        c = conn.cursor()
        sql_odk = (
            "SELECT odkID, odkURL, odkUser, odkPassword, odkFormID, "
            "odkLastRun, odkUseCentral, odkProjectNumber FROM ODK_Conf;"
        )
        try:
            query_odk = c.execute(sql_odk).fetchall()
            conn.close()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise ODKConfigurationError(
                "Problem in database table ODK_Conf..." + str(e)
            )
        odk_id = query_odk[0][0]
        odk_url = query_odk[0][1]
        start_html = odk_url[0:7]
        start_htmls = odk_url[0:8]
        if not (start_html == "http://" or start_htmls == "https://"):
            raise ODKConfigurationError(
                "Problem in database: ODK_Conf.odk_url")
        odk_user = query_odk[0][2]
        odk_password = query_odk[0][3]
        odk_form_id = query_odk[0][4]
        odk_last_run = query_odk[0][5]
        odk_use_central = query_odk[0][6]
        odk_project_number = query_odk[0][7]
        # odkLastRunResult = queryODK[0][6]
        # if not odkLastRunResult in ("success", "fail"):
        #     raise ODKConfigurationError \
        #         ("Problem in database: ODK_Conf.odkLastRunResult")
        odk_last_run_date = datetime.strptime(
            odk_last_run, "%Y-%m-%d_%H:%M:%S"
        ).strftime("%Y/%m/%d")
        odk_last_run_date_prev = (
            datetime.strptime(odk_last_run_date, "%Y/%m/%d")
            - timedelta(days=1)
        ).strftime("%Y/%m/%d")

        nt_odk = namedtuple(
            "nt_odk",
            [
                "odk_id",
                "odk_url",
                "odk_user",
                "odk_password",
                "odk_form_id",
                "odk_last_run",
                # "odkLastRunResult",
                "odk_last_run_date",
                "odk_last_run_date_prev",
                "odk_use_central",
                "odk_project_number",
            ],
        )
        settings_odk = nt_odk(
            odk_id,
            odk_url,
            odk_user,
            odk_password,
            odk_form_id,
            odk_last_run,
            # odkLastRunResult,
            odk_last_run_date,
            odk_last_run_date_prev,
            odk_use_central,
            odk_project_number,
        )

        return settings_odk

    def update_odk_last_run(self) -> None:
        """Update Transfer Database table ODK_Conf.odk_last_run

        """

        conn = self._connect_db()
        c = conn.cursor()
        sql = "UPDATE ODK_Conf SET odkLastRun = ?"
        par = (self.pl_run_date,)
        c.execute(sql, par)
        conn.commit()
        conn.close()

    def insert_event_log(self,
                         values: Tuple[str, str, str]) -> None:
        """Insert new row in Transfer Database table EventLog

        :param values: Event description, event type (e.g., Event, Error,
        Warning, Summary), and date and time of entry
        :type values: Tuple of 3 strings
        """

        conn = self._connect_db()
        c = conn.cursor()
        sql = ("INSERT INTO EventLog (eventDesc, eventType, eventTime)"
               "VALUES (?, ?, ?)")
        c.execute(sql, values)
        conn.commit()
        conn.close()

    def update_table(self,
                     table_name: str,
                     field: Union[str, list],
                     value: Union[str, list]) -> None:
        """Update value(s) into Transfer Database table_name.field(s)

        :param table_name: name of table in Transfer Database
        :type table_name: str
        :param field: field name(s) in table_name
        :type field: str or list of str
        :param value: new values to update in table_name.fields
        :type value: str or list of str
        """

        conn = self._connect_db()
        c = conn.cursor()
        if isinstance(field, str):
            field = [field]
        if isinstance(value, str):
            value = [value]
        valid_table_names = (
            "Pipeline_Conf", "ODK_Conf", "InterVA_Conf",
            "Advanced_InterVA_Conf", "InSilicoVA_Conf",
            "Advanced_InSilicoVA_Conf", "SmartVA_Conf", "SmartVA_Country",
            "DHIS_Conf")
        if table_name not in valid_table_names:
            raise ValueError(f"Unable to access {table_name}")
        else:
            sql_table = f"UPDATE {table_name} "
            sql = sql_table + "SET {}=?"
            for par in zip(field, value):
                c.execute(sql.format(par[0]), (par[1],))
            conn.commit()
            conn.close()

    def get_table_conf(self, table_name: str) -> list:
        """Get values in ODK_Conf table from Transfer Database

        :param table_name: name of table in Transfer Database
        :type table_name: str
        :rtype: list
        """

        conn = self._connect_db()
        c = conn.cursor()
        valid_table_names = (
            "Pipeline_Conf", "ODK_Conf", "InterVA_Conf",
            "Advanced_InterVA_Conf", "InSilicoVA_Conf",
            "Advanced_InSilicoVA_Conf", "SmartVA_Conf", "SmartVA_Country",
            "DHIS_Conf")
        if table_name not in valid_table_names:
            conn.close()
            raise ValueError(f"Unable to access {table_name}")
        else:
            sql = "SELECT * FROM {}"
            table_values = c.execute(sql.format(table_name)).fetchall()
            conn.close()
        # TODO: make the return object a dict where the keys are the field
        # names and the values are the query results
        return table_values

    def get_tables(self) -> list:
        """Get table names from Transfer Database

        :rtype: list
        """

        conn = self._connect_db()
        c = conn.cursor()
        sql_tables = "SELECT name FROM sqlite_master WHERE type='table'"
        query_tables = c.execute(sql_tables).fetchall()
        table_names = [i[0] for i in query_tables]
        conn.close()
        return table_names

    def get_fields(self,
                   table: List[str]) -> List[Tuple[str, str]]:
        """Get field names from table in Transfer Database

        :param table: Name of table
        :type table: str
        :rtype: list of (field name, data type)
        """

        conn = self._connect_db()
        c = conn.cursor()
        sql_fields = f"PRAGMA table_info({table})"
        field_names = c.execute(sql_fields).fetchall()
        field_names = [(i[1], i[2]) for i in field_names]
        conn.close()
        return field_names

    def get_schema(self, table: str) -> list:
        """Get schema from table in Transfer Database

        :param table: Name of table
        :type table: str
        :rtype: list
        """

        conn = self._connect_db()
        c = conn.cursor()
        sql_schema = ("SELECT sql FROM sqlite_master " +
                      f"WHERE type='table' and name='{table}'")
        schema = c.execute(sql_schema).fetchall()
        conn.close()
        return schema

    def get_event_log(self, n_messages: int, recent: bool) -> list:
        """Get rows from EventLog table in Transfer Database

        :param n_messages: Number of messages to retrieve
        :type n_messages: int
        :param recent: Get messages starting from the most recent
        :type: bool
        :rtype: list
        """

        conn = self._connect_db()
        c = conn.cursor()
        sql_log = "SELECT * FROM EventLog ORDER BY eventTime"
        logs_all = c.execute(sql_log).fetchall()
        if recent:
            logs = logs_all[-n_messages:]
        else:
            logs = logs_all[0:n_messages]
        conn.close()
        return logs

    def check_duplicates(self, use_dhis: bool) -> dict:
        """Search for duplicate VA records.

        This method searches for duplicate VA records in ODK export
        file and the Transfer DB.  If duplicates are found, a warning message
        is logged to the EventLog table in the Transfer database and the
        duplicate records are removed from the ODK export file.

        :param use_dhis: Indicator for posting records to DHIS2.  If True, then
        check VA_Org_Unit_Not_Found table for additional duplicate VA records.
        :type use_dhis: bool
        :raises: DatabaseConnectionError, PipelineError
        :return: Number of duplicates found and number of VA records sending to
        openVA.
        :rtype: dict
        """

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        results = {"n_records": None,
                   "n_unique": None,
                   "n_duplicates": None}
        odk_export_path = os.path.join(
            self.working_directory, "ODKFiles", "odk_export_new.csv"
        )
        df_odk = read_csv(odk_export_path)
        df_odk_id = df_odk["meta-instanceID"]
        results["n_records"] = df_odk.shape[0]
        results["n_unique"] = df_odk.shape[0]

        va_ids_list = self._get_va_storage_ids()
        if use_dhis:
            va_ids_no_org_unit = self._get_va_org_unit_not_found_ids()
            va_ids_list.extend(va_ids_no_org_unit)
        va_duplicates = set(df_odk_id).intersection(set(va_ids_list))
        time_fmt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        results["n_duplicates"] = len(va_duplicates)

        conn = self._connect_db()
        c = conn.cursor()
        if results["n_duplicates"] > 0:
            sql_xfer_db = (
                "INSERT INTO EventLog "
                "(eventDesc, eventType, eventTime) VALUES (?, ?, ?)"
            )
            event_desc_part1 = [
                "Removing duplicate records from ODK Export with "
                + "ODK Meta-Instance ID: "
            ] * results["n_duplicates"]
            event_desc_part2 = list(va_duplicates)
            event_desc = [x + y for x, y in
                          zip(event_desc_part1, event_desc_part2)]
            event_type = ["Warning"] * results["n_duplicates"]
            event_time = [time_fmt] * results["n_duplicates"]
            par = list(zip(event_desc, event_type, event_time))
            c.executemany(sql_xfer_db, par)
            conn.commit()
            df_no_duplicates = df_odk[
                ~df_odk["meta-instanceID"].isin(list(va_duplicates))]
            results["n_unique"] = df_no_duplicates.shape[0]
            try:
                df_no_duplicates.to_csv(odk_export_path, index=False)
                conn.close()
            except (PermissionError, OSError) as exc:
                conn.close()
                raise PipelineError(
                    "Error trying to create new CSV file after " +
                    "removing duplicate records in ODK Export"
                ) from exc
        return results

    def _get_va_storage_ids(self) -> List:
        """Get VA IDs from Transfer database table VA_Storage.

        :raises: DatabaseConnectionError
        :rtype: list
        """

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")

        try:
            conn = self._connect_db()
            c = conn.cursor()
            sql = "SELECT id FROM VA_Storage"
            c.execute(sql)
            va_ids = c.fetchall()
            va_ids_list = [j for i in va_ids for j in i]
            conn.close()
        except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
            conn.close()
            raise DatabaseConnectionError(
                "Problem accessing id table VA_Storage..." + str(e)
            )
        return va_ids_list

    def _get_va_org_unit_not_found_ids(self) -> List:
        """Get VA IDs from Transfer database table VA_Org_Unit_Not_Found.

        :raises: DatabaseConnectionError
        :rtype: list
        """

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")

        try:
            conn = self._connect_db()
            c = conn.cursor()
            sql = "SELECT id FROM VA_Org_Unit_Not_Found"
            c.execute(sql)
            va_ids = c.fetchall()
            va_ids_list = [j for i in va_ids for j in i]
            conn.close()
        except (sqlcipher.DatabaseError, sqlcipher.OperationalError) as e:
            conn.close()
            raise DatabaseConnectionError(
                "Problem accessing id table VA_Org_Unit_Not_Found..." + str(e)
            )
        return va_ids_list

    def config_openva(self,
                      algorithm: str) -> NamedTuple:
        """Query OpenVA configuration settings from database.

        This method is intended to receive its input (a Connection object)
        from :meth:`TransferDB.connect_db() <connect_db>`, which establishes a
        connection to a database with the Pipeline configuration settings.  It
        sets up the configuration for all of the VA algorithms included in the
        R package openVA.  The output from
        :meth:`config_openva() <config_openva>` serves as an input to the
        n method
        :meth:`OpenVA.setAlgorithmParameters() 
        <openva_pipeline.odk.ODK.setAlgorithmParameters>`.
        This is a wrapper function that calls :meth:`_config_interva`,
        :meth:`_config_insilicova`, and :meth:`_config_smartva` to actually
          pull configuration settings from the database.
        :param algorithm: VA algorithm used by R package openVA
        :type algorithm: str
        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        if algorithm == "InterVA":
            settings_interva = self._config_interva()
            return settings_interva
        elif algorithm == "InSilicoVA":
            settings_insilicova = self._config_insilicova()
            return settings_insilicova
        elif algorithm == "SmartVA":
            settings_smartva = self._config_smartva()
            return settings_smartva
        else:
            raise PipelineConfigurationError(
                "Not an acceptable parameter for 'algorithm'."
            )

    def _config_interva(self) -> NamedTuple:
        """Query OpenVA configuration settings from database.

        This method is called by config_openva when the VA algorithm is either
        InterVA4 or InterVA5.

        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        conn = self._connect_db()
        c = conn.cursor()
        try:
            sql_interva = "SELECT version, HIV, Malaria FROM InterVA_Conf;"
            query_interva = c.execute(sql_interva).fetchall()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table InterVA_Conf..." + str(e)
            )

        # Database Table: InterVA_Conf
        interva_version = query_interva[0][0]
        if interva_version not in ("4", "5"):
            conn.close()
            raise OpenVAConfigurationError(
                "Problem in database: InterVA_Conf.version "
                "(valid options: '4' or '5')."
            )
        interva_hiv = query_interva[0][1]
        if interva_hiv not in ("v", "l", "h"):
            conn.close()
            raise OpenVAConfigurationError(
                "Problem in database: InterVA_Conf.HIV "
                "(valid options: 'v', 'l', or 'h')."
            )
        interva_malaria = query_interva[0][2]
        if interva_malaria not in ("v", "l", "h"):
            conn.close()
            raise OpenVAConfigurationError(
                "Problem in database: InterVA_Conf.Malaria "
                "(valid options: 'v', 'l', or 'h')."
            )

        # Database Table: Advanced_InterVA_Conf
        try:
            sql_advanced_interva = (
                "SELECT output, append, groupcode, "
                "replicate, replicate_bug1, replicate_bug2 "
                "FROM Advanced_InterVA_Conf;"
            )
            query_advanced_interva = c.execute(sql_advanced_interva).fetchall()
            conn.close()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table Advanced_InterVA_Conf..." + str(e)
            )

        interva_output = query_advanced_interva[0][0]
        if interva_output not in ("classic", "extended"):
            raise OpenVAConfigurationError(
                "Problem in database: Advanced_InterVA_Conf.output."
            )
        interva_append = query_advanced_interva[0][1]
        if interva_append not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: Advanced_InterVA_Conf.append."
            )
        interva_groupcode = query_advanced_interva[0][2]
        if interva_groupcode not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: Advanced_InterVA_Conf.groupcode."
            )
        interva_replicate = query_advanced_interva[0][3]
        if interva_replicate not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: Advanced_InterVA_Conf.replicate."
            )
        interva_replicate_bug1 = query_advanced_interva[0][4]
        if interva_replicate_bug1 not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: Advanced_InterVA_Conf.replicate_bug1."
            )
        interva_replicate_bug2 = query_advanced_interva[0][5]
        if interva_replicate_bug2 not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: Advanced_InterVA_Conf.replicate_bug2."
            )

        nt_interva = namedtuple(
            "nt_interva",
            [
                "interva_version",
                "interva_hiv",
                "interva_malaria",
                "interva_output",
                "interva_append",
                "interva_groupcode",
                "interva_replicate",
                "interva_replicate_bug1",
                "interva_replicate_bug2",
            ],
        )
        settings_interva = nt_interva(
            interva_version,
            interva_hiv,
            interva_malaria,
            interva_output,
            interva_append,
            interva_groupcode,
            interva_replicate,
            interva_replicate_bug1,
            interva_replicate_bug2,
        )
        return settings_interva

    def _config_insilicova(self) -> NamedTuple:
        """Query OpenVA configuration settings from database.

        This method is called by config_openva when the VA algorithm is
        InSilicoVA.

        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        conn = self._connect_db()
        c = conn.cursor()

        # Database Table: InSilicoVA_Conf
        try:
            sql_insilicova = "SELECT data_type, Nsim FROM InSilicoVA_Conf;"
            query_insilicova = c.execute(sql_insilicova).fetchall()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table InSilicoVA_Conf..." + str(e)
            )

        insilicova_data_type = query_insilicova[0][0]
        if insilicova_data_type not in ("WHO2012", "WHO2016"):
            conn.close()
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.data_type "
                "(valid options: 'WHO2012' or 'WHO2016')."
            )
        insilicova_nsim = query_insilicova[0][1]
        if insilicova_nsim in ("", None):
            conn.close()
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.Nsim")

        # Database Table: Advanced_InSilicoVA_Conf
        try:
            sql_advanced_insilicova = (
                "SELECT isNumeric, updateCondProb, "
                "keepProbbase_level, CondProb, "
                "CondProbNum, datacheck, "
                "datacheck_missing, external_sep, thin, "
                "burnin, auto_length, conv_csmf, "
                "jump_scale, levels_prior, "
                "levels_strength, trunc_min, trunc_max, "
                "subpop, java_option, seed, phy_code, "
                "phy_cat, phy_unknown, phy_external, "
                "phy_debias, exclude_impossible_cause, "
                "no_is_missing, indiv_CI, groupcode "
                "FROM Advanced_InSilicoVA_Conf;"
            )
            query_advanced_insilicova = c.execute(
                sql_advanced_insilicova).fetchall()
            conn.close()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table Advanced_InSilicoVA_Conf..." +
                str(e)
            )

        insilicova_is_numeric = query_advanced_insilicova[0][0]
        if insilicova_is_numeric not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.isNumeric "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_update_cond_prob = query_advanced_insilicova[0][1]
        if insilicova_update_cond_prob not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.updateCondProb "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_keep_probbase_level = query_advanced_insilicova[0][2]
        if insilicova_keep_probbase_level not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.keepProbbase_level "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_cond_prob = query_advanced_insilicova[0][3]
        if insilicova_cond_prob in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.CondProb "
                "(valid options: name of R object)."
            )
        insilicova_cond_prob_num = query_advanced_insilicova[0][4]
        if insilicova_cond_prob_num != "NULL":
            try:
                float_cond_prob_num = float(insilicova_cond_prob_num)
            except ValueError:
                raise OpenVAConfigurationError(
                    "Problem in database: InSilicoVA_Conf.CondProbNum "
                    "(must be between '0' and '1')."
                )
            if not (0 <= float_cond_prob_num <= 1):
                raise OpenVAConfigurationError(
                    "Problem in database: InSilicoVA_Conf.CondProbNum "
                    "(must be between '0' and '1')."
                )
        insilicova_datacheck = query_advanced_insilicova[0][5]
        if insilicova_datacheck not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.datacheck "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_datacheck_missing = query_advanced_insilicova[0][6]
        if insilicova_datacheck_missing not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.datacheck_missing "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_external_sep = query_advanced_insilicova[0][7]
        if insilicova_external_sep not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.external_sep "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_thin = query_advanced_insilicova[0][8]
        try:
            thin_float = float(insilicova_thin)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.thin " 
                "(must be 'thin' > 0."
            )
        if thin_float <= 0:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.thin " 
                "(must be 'thin' > 0."
            )
        insilicova_burnin = query_advanced_insilicova[0][9]
        try:
            burnin_float = float(insilicova_burnin)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.burnin " 
                "(must be 'burnin' > 0."
            )
        if burnin_float <= 0:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.burnin " 
                "(must be 'burnin' > 0."
            )
        insilicova_auto_length = query_advanced_insilicova[0][10]
        if insilicova_auto_length not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.auto_length "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_conv_csmf = query_advanced_insilicova[0][11]
        try:
            float_conv_csmf = float(insilicova_conv_csmf)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.conv_csmf "
                "(must be between '0' and '1')."
            )
        if not (0 <= float_conv_csmf <= 1):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.conv_csmf "
                "(must be between '0' and '1')."
            )
        insilicova_jump_scale = query_advanced_insilicova[0][12]
        try:
            float_jump_scale = float(insilicova_jump_scale)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.jump_scale "
                "(must be greater than '0')."
            )
        if float_jump_scale <= 0:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.jump_scale "
                "(must be greater than '0')."
            )
        insilicova_levels_prior = query_advanced_insilicova[0][13]
        if insilicova_levels_prior in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.levels_prior "
                "(valid options: name of R object)."
            )
        insilicova_levels_strength = query_advanced_insilicova[0][14]
        try:
            float_levels_strength = float(insilicova_levels_strength)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.levels_strength "
                "(must be greater than '0')."
            )
        if float_levels_strength <= 0:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.levels_strength "
                "(must be greater than '0')."
            )
        insilicova_trunc_min = query_advanced_insilicova[0][15]
        try:
            float_trunc_min = float(insilicova_trunc_min)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.trunc_min "
                "(must be between '0' and '1')."
            )
        if not (0 <= float_trunc_min <= 1):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.trunc_min "
                "(must be between '0' and '1')."
            )
        insilicova_trunc_max = query_advanced_insilicova[0][16]
        try:
            float_trunc_max = float(insilicova_trunc_max)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.trunc_max "
                "(must be between '0' and '1')."
            )
        if not (0 <= float_trunc_max <= 1):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.trunc_max "
                "(must be between '0' and '1')."
            )
        insilicova_subpop = query_advanced_insilicova[0][17]
        if insilicova_subpop in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.subpop "
                "(valid options: name of R object)."
            )
        insilicova_java_option = query_advanced_insilicova[0][18]
        if insilicova_java_option == "" or len(insilicova_java_option) < 6:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.java_option "
                "(should look like '-Xmx1g')."
            )
        jo_length = len(insilicova_java_option)
        jo_last_char = insilicova_java_option[(jo_length - 1)]
        jo_first4_char = insilicova_java_option[0:4]
        jo_mem_size = insilicova_java_option[4:(jo_length - 1)]
        if jo_first4_char != "-Xmx":
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.java_option "
                "(should start with '-Xmx')."
            )
        if jo_last_char not in ("m", "g"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.java_option "
                "(should end with 'g' for gigabytes or 'm' for megabytes)."
            )
        try:
            float_jo_mem_size = float(jo_mem_size)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.java_option "
                "(should look like '-Xmx1g')."
            )
        if float_jo_mem_size <= 0:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.java_option "
                "(should look like '-Xmx1g')."
            )
        insilicova_seed = query_advanced_insilicova[0][19]
        try:
            float(insilicova_seed)
        except ValueError:
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.seed "
                "(must be between a number; preferably an integer)."
            )
        insilicova_phy_code = query_advanced_insilicova[0][20]
        if insilicova_phy_code in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.phy_code "
                "(valid options: name of R object)."
            )
        insilicova_phy_cat = query_advanced_insilicova[0][21]
        if insilicova_phy_cat in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.phy_cat "
                "(valid options: name of R object)."
            )
        insilicova_phy_unknown = query_advanced_insilicova[0][22]
        if insilicova_phy_unknown in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.phy_unknown "
                "(valid options: name of R object)."
            )
        insilicova_phy_external = query_advanced_insilicova[0][23]
        if insilicova_phy_external in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.phy_external "
                "(valid options: name of R object)."
            )
        insilicova_phy_debias = query_advanced_insilicova[0][24]
        if insilicova_phy_debias in ("", None):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.phy_debias "
                "(valid options: name of R object)."
            )
        insilicova_exclude_impossible_cause = query_advanced_insilicova[0][25]
        if insilicova_exclude_impossible_cause not in ("subset", "all",
                                                       "InterVA", "none"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.exclude_impossible_cause"
                " (valid options: 'subset', 'all', 'InterVA', and 'none')."
            )
        insilicova_no_is_missing = query_advanced_insilicova[0][26]
        if insilicova_no_is_missing not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.no_is_missing "
                "(valid options: 'TRUE' or 'FALSE')."
            )
        insilicova_indiv_ci = query_advanced_insilicova[0][27]
        if insilicova_indiv_ci != "NULL":
            try:
                float_indiv_ci = float(insilicova_indiv_ci)
            except ValueError:
                raise OpenVAConfigurationError(
                    "Problem in database: InSilicoVA_Conf.indiv_CI "
                    "(must be between '0' and '1')."
                )
            if not (0 < float_indiv_ci < 1):
                raise OpenVAConfigurationError(
                    "Problem in database: InSilicoVA_Conf.indiv_CI "
                    "(must be between '0' and '1')."
                )
        insilicova_groupcode = query_advanced_insilicova[0][28]
        if insilicova_groupcode not in ("TRUE", "FALSE"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.groupcode "
                "(valid options: 'TRUE' or 'FALSE')."
            )

        nt_insilicova = namedtuple(
            "nt_insilicova",
            [
                "insilicova_data_type",
                "insilicova_nsim",
                "insilicova_is_numeric",
                "insilicova_update_cond_prob",
                "insilicova_keep_probbase_level",
                "insilicova_cond_prob",
                "insilicova_cond_prob_num",
                "insilicova_datacheck",
                "insilicova_datacheck_missing",
                "insilicova_external_sep",
                "insilicova_thin",
                "insilicova_burnin",
                "insilicova_auto_length",
                "insilicova_conv_csmf",
                "insilicova_jump_scale",
                "insilicova_levels_prior",
                "insilicova_levels_strength",
                "insilicova_trunc_min",
                "insilicova_trunc_max",
                "insilicova_subpop",
                "insilicova_java_option",
                "insilicova_seed",
                "insilicova_phy_code",
                "insilicova_phy_cat",
                "insilicova_phy_unknown",
                "insilicova_phy_external",
                "insilicova_phy_debias",
                "insilicova_exclude_impossible_cause",
                "insilicova_no_is_missing",
                "insilicova_indiv_ci",
                "insilicova_groupcode",
            ],
        )
        settings_insilicova = nt_insilicova(
            insilicova_data_type,
            insilicova_nsim,
            insilicova_is_numeric,
            insilicova_update_cond_prob,
            insilicova_keep_probbase_level,
            insilicova_cond_prob,
            insilicova_cond_prob_num,
            insilicova_datacheck,
            insilicova_datacheck_missing,
            insilicova_external_sep,
            insilicova_thin,
            insilicova_burnin,
            insilicova_auto_length,
            insilicova_conv_csmf,
            insilicova_jump_scale,
            insilicova_levels_prior,
            insilicova_levels_strength,
            insilicova_trunc_min,
            insilicova_trunc_max,
            insilicova_subpop,
            insilicova_java_option,
            insilicova_seed,
            insilicova_phy_code,
            insilicova_phy_cat,
            insilicova_phy_unknown,
            insilicova_phy_external,
            insilicova_phy_debias,
            insilicova_exclude_impossible_cause,
            insilicova_no_is_missing,
            insilicova_indiv_ci,
            insilicova_groupcode,
        )
        return settings_insilicova

    def _config_smartva(self) -> NamedTuple:
        """Query OpenVA configuration settings from database.

        This method is called by config_openva when the VA algorithm is
        SmartVA.

        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        conn = self._connect_db()
        c = conn.cursor()

        try:
            sql_smartva = (
                "SELECT country, hiv, malaria, hce, freetext, "
                "figures, language FROM SmartVA_Conf;"
            )
            query_smartva = c.execute(sql_smartva).fetchall()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table SmartVA_Conf..." + str(e)
            )

        try:
            sql_country_list = "SELECT abbrev FROM SmartVA_Country;"
            query_country_list = c.execute(sql_country_list).fetchall()
            conn.close()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table SmartVA_Country..." + str(e)
            )

        smartva_country = query_smartva[0][0]
        if smartva_country not in [j for i in query_country_list for j in i]:
            raise OpenVAConfigurationError(
                "Problem in database: SmartVA_Conf.country")
        smartva_hiv = query_smartva[0][1]
        if smartva_hiv not in ("True", "False"):
            raise OpenVAConfigurationError(
                "Problem in database: SmartVA_Conf.hiv")
        smartva_malaria = query_smartva[0][2]
        if smartva_malaria not in ("True", "False"):
            raise OpenVAConfigurationError(
                "Problem in database: SmartVA_Conf.malaria")
        smartva_hce = query_smartva[0][3]
        if smartva_hce not in ("True", "False"):
            raise OpenVAConfigurationError(
                "Problem in database: SmartVA_Conf.hce")
        smartva_freetext = query_smartva[0][4]
        if smartva_freetext not in ("True", "False"):
            raise OpenVAConfigurationError(
                "Problem in database: SmartVA_Conf.freetext")
        smartva_figures = query_smartva[0][5]
        if smartva_figures not in ("True", "False"):
            raise OpenVAConfigurationError(
                "Problem in database: SmartVA_Conf.figures")
        smartva_language = query_smartva[0][6]
        if smartva_language not in ("english", "chinese", "spanish"):
            raise OpenVAConfigurationError(
                "Problem in database: SmartVA_Conf.language")

        nt_smartva = namedtuple(
            "nt_smartva",
            [
                "smartva_country",
                "smartva_hiv",
                "smartva_malaria",
                "smartva_hce",
                "smartva_freetext",
                "smartva_figures",
                "smartva_language",
            ],
        )
        settings_smartva = nt_smartva(
            smartva_country,
            smartva_hiv,
            smartva_malaria,
            smartva_hce,
            smartva_freetext,
            smartva_figures,
            smartva_language,
        )
        return settings_smartva

    def config_dhis(self,
                    algorithm: str) -> List[Union[NamedTuple, Dict]]:
        """Query DHIS configuration settings from database.

        This method is intended to be used in conjunction with (1)
        :meth:`TransferDB.connect_db()<connect_db>`, which establishes a
        connection to a database with the Pipeline configuration settings;
        and (2) :meth:`DHIS.connect()<openva_pipeline.dhis.DHIS.connect>`,
        which establishes a connection to a DHIS server.  Thus,
        TransferDB.config_dhis() gets its input from
        :meth:`TransferDB.connect_db()<connect_db>` and the output from
        TransferDB.config() is a valid argument for
        :meth:`DHIS.connect()<openva_pipeline.dhis.DHIS.connect>`

        :param algorithm: VA algorithm used by R package openVA
        :type algorithm: str
        :returns: First item contains all parameters for
          :meth:`DHIS.connect() <openva_pipeline.dhis.DHIS.connect>`,
          and the second item contains the causes of death used by the
          VA Program (in DHIS2)
        :rtype: list [named tuple, dict]
        :raises: DHISConfigurationError
        """

        conn = self._connect_db()
        c = conn.cursor()
        try:
            sql_dhis = (
                "SELECT dhisURL, dhisUser, dhisPassword, "
                "dhisOrgUnit, dhisPostRoot " 
                "FROM DHIS_Conf;"
            )
            query_dhis = c.execute(sql_dhis).fetchall()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise PipelineConfigurationError(
                "Problem in database table DHIS_Conf..." + str(e)
            )

        if algorithm == "SmartVA":
            sql_cod_codes = (
                "SELECT codName, codCode FROM COD_Codes_DHIS WHERE "
                "codSource = 'Tariff'"
            )
        else:
            sql_cod_codes = (
                "SELECT codName, codCode FROM COD_Codes_DHIS WHERE " 
                "codSource = 'WHO'"
            )
        try:
            query_cod_codes = c.execute(sql_cod_codes).fetchall()
            dhis_cod_codes = dict(query_cod_codes)
            conn.close()
        except sqlcipher.OperationalError as e:
            conn.close()
            raise DHISConfigurationError(
                "Problem in database table COD_Codes_DHIS..." + str(e)
            )

        dhis_url = query_dhis[0][0]
        start_html = dhis_url[0:7]
        start_htmls = dhis_url[0:8]
        if not (start_html == "http://" or start_htmls == "https://"):
            raise DHISConfigurationError(
                "Problem in database: DHIS_Conf.dhisURL")
        dhis_user = query_dhis[0][1]
        if dhis_user == "" or dhis_user is None:
            raise DHISConfigurationError(
                "Problem in database: DHIS_Conf.dhisUser (is empty)"
            )
        dhis_password = query_dhis[0][2]
        if dhis_password == "" or dhis_password is None:
            raise DHISConfigurationError(
                "Problem in database: DHIS_Conf.dhisPassword (is empty)"
            )
        dhis_org_unit = query_dhis[0][3]
        if dhis_org_unit == "" or dhis_org_unit is None:
            raise DHISConfigurationError(
                "Problem in database: DHIS_Conf.dhisOrgUnit (is empty)"
            )
        dhis_post_root = query_dhis[0][4]

        nt_dhis = namedtuple(
            "nt_dhis", ["dhis_url", "dhis_user",
                        "dhis_password", "dhis_org_unit", "dhis_post_root"]
        )
        settings_dhis = nt_dhis(dhis_url, dhis_user, dhis_password,
                                dhis_org_unit, dhis_post_root)

        return [settings_dhis, dhis_cod_codes]

    def store_va(self, dhis_tracker: bool = False) -> None:
        """Store VA records in Transfer database.

        This method is intended to be used in conjunction with the
        :class:`DHIS <openva_pipeline.dhis.DHIS>` class, which prepares the
        records into the proper format for storage in the Transfer database.

        :param dhis_tracker: Indicator of using DHIS2 VA tracker program
        :type dhis_tracker: bool
        :raises: PipelineError, DatabaseConnectionError
        """

        if self.working_directory is None:
            raise PipelineError("Need to run Pipeline.config().")
        conn = self._connect_db()
        c = conn.cursor()
        new_storage_path = os.path.join(
            self.working_directory, "OpenVAFiles", "new_storage.csv"
        )
        df_new_storage = read_csv(new_storage_path)
        time_fmt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        try:
            for row_dict in df_new_storage.to_dict(orient="records"):
                # xfer_db_id = row_dict["id"]
                # xfer_db_outcome = row_dict["pipelineOutcome"]
                non_data_cols = [k for k in row_dict.keys()
                                 if k.startswith("org_unit_col")]
                non_data_cols.extend(["sex", "dob", "dod", "age",
                                      "cod", "metadataCode",
                                      "odkMetaInstanceID", "pipelineOutcome"])
                va_data = tuple(
                    v for k, v in row_dict.items() if k not in non_data_cols)
                xfer_db_record = dumps(va_data)
                sql_list = ["INSERT INTO VA_Storage"]
                if dhis_tracker:
                    sql_add = (
                        "(id, outcome, record, dateEntered, "
                        "dhisOrgUnit, eventID, teiID) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)")
                else:
                    sql_add = (
                        "(id, outcome, record, dateEntered, "
                        "dhisOrgUnit, eventID) "
                        "VALUES (?, ?, ?, ?, ?, ?)")
                sql_list.append(sql_add)
                sql_xfer_db = " ".join(sql_list)
                par = [row_dict["id"],
                       row_dict["pipelineOutcome"],
                       sqlite3.Binary(xfer_db_record),
                       time_fmt,
                       row_dict["dhis_org_unit"],
                       row_dict["event_id"]]
                if dhis_tracker:
                    par.append(row_dict["tei_id"])
                c.execute(sql_xfer_db, par)
            conn.commit()
            conn.close()
        except (sqlcipher.OperationalError, sqlcipher.IntegrityError) as e:
            conn.close()
            raise DatabaseConnectionError(
                "Problem storing VA record to Transfer DB..." + str(e))

    def store_single_va(self,
                        va_dict: Dict,
                        org_unit_id: str,
                        log_summary: Dict,
                        dhis_tracker: bool = False) -> None:
        """Store a single VA record in Transfer database table VA_Storage.

        This method is intended to be used in conjunction with the
        :class:`DHIS <openva_pipeline.dhis.DHIS>` class, which prepares the
        records into the proper format for storage in the Transfer database.

        :param va_dict: VA record
        :type va_dict: dict
        :param org_unit_id: DHIS2 organisation unit ID
        :type org_unit_id: str
        :param log_summary: Parsed log from DHIS2 post
        :type log_summary: dict
        :param dhis_tracker: Indicator of using DHIS2 VA tracker program
        :type dhis_tracker: bool
        :raises: PipelineError, DatabaseConnectionError
        """

        conn = self._connect_db()
        c = conn.cursor()

        time_fmt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        try:
            non_data_cols = [k for k in va_dict.keys()
                             if k.startswith("org_unit_col")]
            non_data_cols.extend(["sex", "dob", "dod", "age",
                                  "cod", "metadataCode", "odkMetaInstanceID"])
            va_data = tuple(
                v for k, v in va_dict.items() if k not in non_data_cols)
            xfer_db_record = dumps(va_data)
            sql_list = ["INSERT INTO VA_Storage"]
            if dhis_tracker:
                sql_add = (
                    "(id, outcome, record, dateEntered, "
                    "dhisOrgUnit, eventID, teiID) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)")
            else:
                sql_add = (
                    "(id, outcome, record, dateEntered, "
                    "dhisOrgUnit, eventID) "
                    "VALUES (?, ?, ?, ?, ?, ?)")
            sql_list.append(sql_add)
            sql_xfer_db = " ".join(sql_list)
            par = [va_dict["id"],
                   "Pushed to DHIS2",
                   sqlite3.Binary(xfer_db_record),
                   time_fmt,
                   org_unit_id,
                   log_summary["event_id"]
                   ]
            if dhis_tracker:
                par.append(log_summary["tei_id"])
            c.execute(sql_xfer_db, par)
            conn.commit()
            conn.close()
        except (sqlcipher.OperationalError, sqlcipher.IntegrityError) as e:
            conn.close()
            raise DatabaseConnectionError(
                "Problem storing VA record to Transfer DB..." + str(e))

    def store_no_ou_va(self,
                       va_record: dict,
                       eav: DataFrame,
                       data_ou: str) -> None:
                       # dhis_ou: str) -> None:
        """Store VA record without valid organisation unit (ou) in Transfer
        database table VA_Org_Unit_Not_Found.

        :param va_record: VA record processed by openVA along with cause and
        metadata
        :type: dict:
        :param eav: VA record in EAV format (Entity, Attribute, Value)
        prepared by openVA
        :type: DataFrame
        :param data_ou: Organisation unit (for DHIS2) found in data
        :type: str
        :param dhis_ou: Organisation unit the pipeline found and wanted to use
        for posting to DHIS2
        :type: str

        :raises: PipelineError, DatabaseConnectionError
        """

        if self.working_directory is None:
            raise PipelineError("Need to run Pipeline.config().")
        conn = self._connect_db()
        c = conn.cursor()

        time_fmt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        try:
            va_id = va_record["id"]
            # va_record_blob = dumps(va_record)
            # eav_blob = dumps(eav)
            va_json = json.dumps(va_record)
            eav_flat = eav.pivot(index="ID",
                                 columns="Attribute",
                                 values="Value")
            eav_dict = eav_flat.to_dict(orient="records")[0]
            eav_json = json.dumps(eav_dict)
            sql_xfer_db = (
                "INSERT INTO VA_Org_Unit_Not_Found "
                "(id, eventBlob, evaBlob, dataOrgUnit, dhisOrgUnit, "
                "dateEntered, fixed) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            par = [va_id,
                   sqlite3.Binary(va_json.encode("utf-8")),
                   sqlite3.Binary(eav_json.encode("utf-8")),
                   data_ou,
                   #dhis_ou,
                   "No Valid Org Unit Found",
                   time_fmt,
                   "False"]
            c.execute(sql_xfer_db, par)
            conn.commit()
            conn.close()
        except (sqlcipher.OperationalError, sqlcipher.IntegrityError) as e:
            conn.close()
            raise DatabaseConnectionError(
                "Problem storing VA record to Transfer DB " +
                "(VA_Org_Unit_Not_Found table)... "
                + str(e))

    def get_no_ou_va(self, va_id: str = None) -> Dict:
        """Get the VA IDs and org unit data columns for records that do not
        have a valid DHIS2 org unit assignment; or select the eventBlob and
        evaBlob if the va_id is provided."""

        conn = self._connect_db()
        c = conn.cursor()
        if va_id:
            sql = ("SELECT eventBlob, evaBlob FROM VA_Org_Unit_Not_Found "
                   "WHERE id = ?")
            query = c.execute(sql, (va_id,)).fetchall()
            va_dict = json.loads(query[0][0])
            eav_dict = json.loads(query[0][1])
            eav_df = DataFrame.from_dict(eav_dict,
                                         orient="index")
            eav_df.reset_index(inplace=True)
            eav_df.insert(loc=0, column="ID", value=va_id)
            eav_df.rename(columns={"index": "Attribute", 0: "Value"})
            va_data = {"va_dict": va_dict,
                       "eav_dataframe": eav_df}
            conn.close()
            return va_data
        else:
            sql = "SELECT id, dataOrgUnit FROM VA_Org_Unit_Not_Found"
            table_values = c.execute(sql).fetchall()
            id_ou_dict = {va_id: ou for va_id, ou in table_values}
            conn.close()
            return id_ou_dict

    def remove_no_ou_va(self, va_id: str) -> None:
        """Remove the VA record from Transfer database table
        VA_Org_Unit_Not_Found."""

        conn = self._connect_db()
        c = conn.cursor()
        sql = "DELETE FROM VA_Org_Unit_Not_Found WHERE id = ?"
        c.execute(sql, (va_id,))
        conn.commit()
        conn.close()

    def make_pipeline_dirs(self) -> None:
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

        odk_path = os.path.join(self.working_directory, "ODKFiles")
        openva_path = os.path.join(self.working_directory, "OpenVAFiles")
        dhis_path = os.path.join(self.working_directory, "DHIS")

        if not os.path.isdir(odk_path):
            try:
                os.makedirs(odk_path)
            except OSError as e:
                raise PipelineError("Unable to create directory " + odk_path +
                                    str(e))
        if not os.path.isdir(openva_path):
            try:
                os.makedirs(openva_path)
            except OSError as e:
                raise PipelineError("Unable to create directory " +
                                    openva_path + str(e))
        if not os.path.isdir(dhis_path):
            try:
                os.makedirs(dhis_path)
            except OSError as e:
                raise PipelineError("Unable to create directory " +
                                    dhis_path + str(e))

    def clean_odk(self) -> None:
        """Remove ODK Briefcase Export files."""

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        odk_export_new_path = os.path.join(
            self.working_directory, "ODKFiles", "odk_export_new.csv"
        )
        odk_export_prev_path = os.path.join(
            self.working_directory, "ODKFiles", "odk_export_prev.csv"
        )
        if os.path.isfile(odk_export_new_path):
            os.remove(odk_export_new_path)
        if os.path.isfile(odk_export_prev_path):
            os.remove(odk_export_prev_path)

    def clean_openva(self) -> None:
        """Remove openVA files with COD results."""

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        pcva_input_path = os.path.join(
            self.working_directory, "OpenVAFiles", "pycrossva_input.csv"
        )
        openva_input_path = os.path.join(
            self.working_directory, "OpenVAFiles", "openva_input.csv"
        )
        record_storage_path = os.path.join(
            self.working_directory, "OpenVAFiles", "record_storage.csv"
        )
        new_storage_path = os.path.join(
            self.working_directory, "OpenVAFiles", "new_storage.csv"
        )
        eva_path = os.path.join(
            self.working_directory, "OpenVAFiles", "entity_attribute_value.csv"
        )
        if os.path.isfile(pcva_input_path):
            os.remove(pcva_input_path)
        if os.path.isfile(openva_input_path):
            os.remove(openva_input_path)
        if os.path.isfile(record_storage_path):
            os.remove(record_storage_path)
        if os.path.isfile(new_storage_path):
            os.remove(new_storage_path)
        if os.path.isfile(eva_path):
            os.remove(eva_path)

    def clean_dhis(self) -> None:
        """Remove DHIS2 blob files."""

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        rmtree(os.path.join(self.working_directory, "DHIS", "blobs"))
