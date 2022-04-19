"""
openva_pipeline.transfer_db
-----------------

This module handles interactions with the Transfer database.
"""

import os
from shutil import rmtree
from collections import namedtuple
from datetime import datetime, timedelta
import sqlite3
from pickle import dumps
from pandas import read_csv
from pysqlcipher3 import dbapi2 as sqlcipher

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
    :type pl_run_date: datetime
    """

    def __init__(self,
                 db_file_name: str,
                 db_directory: str,
                 db_key: str,
                 pl_run_date: datetime):

        self.db_file_name = db_file_name
        self.db_directory = db_directory
        self.db_key = db_key
        self.db_path = os.path.join(db_directory, db_file_name)
        self.working_directory = None
        self.pl_run_date = pl_run_date

    def connect_db(self):
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

    def config_pipeline(self, conn):
        """Grabs Pipeline configuration settings.

        This method queries the Pipeline_Conf table in Transfer database and
        returns a tuple with attributes (1) algorithmMetadataCode; (2)
        codSource; (3) algorithm; and (4) workingDirectory.

        :returns: Arguments needed to configure the OpenVA Pipeline
          algorithmMetadataCode - attribute describing VA data
          codSource - attribute detailing the source of the Cause of Death list
          algorithm - attribute indicating which VA algorithm to use
          workingDirectory - attribute indicating the working directory
        :rtype: (named) tuple
        :raises: PipelineConfigurationError
        """

        c = conn.cursor()
        try:
            sql_pipeline = (
                "SELECT algorithmMetadataCode, codSource, algorithm, "
                "workingDirectory FROM Pipeline_Conf;"
            )
            query_pipeline = c.execute(sql_pipeline).fetchall()
        except sqlcipher.OperationalError as e:
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

    @staticmethod
    def config_odk(conn):
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

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :returns: Contains all parameters for
          :meth:`ODK.briefcase()<openva_pipeline.odk.ODK.briefcase>`.
        :rtype: (named) tuple
        :raises: ODKConfigurationError
        """

        c = conn.cursor()
        sql_odk = (
            "SELECT odkID, odkURL, odkUser, odkPassword, odkFormID, "
            "odkLastRun, odkUseCentral, odkProjectNumber FROM ODK_Conf;"
        )
        try:
            query_odk = c.execute(sql_odk).fetchall()
        except sqlcipher.OperationalError as e:
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

    @staticmethod
    def update_odk_last_run(conn, pl_run_date):
        """Update Transfer Database table ODK_Conf.odk_last_run

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param pl_run_date: Date when pipeline started latest run
        :type pl_run_date: date (YYYY-MM-DD_hh:mm:ss)
        """

        c = conn.cursor()
        sql = "UPDATE ODK_Conf SET odkLastRun = ?"
        par = (pl_run_date,)
        c.execute(sql, par)
        conn.commit()

    @staticmethod
    def update_odk_conf(conn, field, value):
        """Update value(s) into Transfer Database ODK_Conf.field(s)

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param field: field name(s) in table ODK_Conf
        :type field: str or list of str
        :param value: list of new values used to update in ODK_Conf.fields
        """

        c = conn.cursor()
        sql = "UPDATE ODK_Conf SET {}=?"
        for par in zip(field, value):
            c.execute(sql.format(par[0]), (par[1],))
        conn.commit()

    @staticmethod
    def update_dhis_conf(conn, field, value):
        """Update value(s) into Transfer Database DHIS_Conf.field(s)

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param field: field name(s) in table DHIS_Conf
        :type field: str or list of str
        :param value: list of new values to update in DHIS_Conf.fields
        """

        c = conn.cursor()
        sql = "UPDATE DHIS_Conf SET {}=?"
        for par in zip(field, value):
            c.execute(sql.format(par[0]), (par[1],))
        conn.commit()

    @staticmethod
    def update_pipeline_conf(conn, field, value):
        """Update value(s) into Transfer Database Pipeline_Conf.field(s)

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param field: field name(s) in table Pipeline_Conf
        :type field: str or list of str
        :param value: list of new values to update in Pipeline_Conf.fields
        """

        c = conn.cursor()
        sql = "UPDATE Pipeline_Conf SET {}=?"
        for par in zip(field, value):
            c.execute(sql.format(par[0]), (par[1],))
        conn.commit()

    @staticmethod
    def get_odk_conf(conn):
        """Get values in ODK_Conf table from Transfer Database

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :rtype: list
        """

        c = conn.cursor()
        sql_odk = "SELECT * FROM ODK_Conf"
        odk_values = c.execute(sql_odk).fetchall()
        return odk_values

    @staticmethod
    def get_dhis_conf(conn):
        """Get values in DHIS_Conf table from Transfer Database

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :rtype: list
        """

        c = conn.cursor()
        sql_dhis = "SELECT * FROM DHIS_Conf"
        dhis_values = c.execute(sql_dhis).fetchall()
        return dhis_values

    @staticmethod
    def get_pipeline_conf(conn):
        """Get values in Pipeline_Conf table from Transfer Database

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :rtype: list
        """

        c = conn.cursor()
        sql_pipeline = "SELECT * FROM Pipeline_Conf"
        pipeline_values = c.execute(sql_pipeline).fetchall()
        return pipeline_values

    @staticmethod
    def get_tables(conn):
        """Get table names from Transfer Database

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :rtype: list
        """

        c = conn.cursor()
        sql_tables = "SELECT name FROM sqlite_master WHERE type='table'"
        query_tables = c.execute(sql_tables).fetchall()
        table_names = [i[0] for i in query_tables]
        return table_names

    @staticmethod
    def get_fields(conn, table):
        """Get field names from table in Transfer Database

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param table: Name of table
        :type table: str
        :rtype: list of (field name, data type)
        """

        c = conn.cursor()
        sql_fields = f"PRAGMA table_info({table})"
        field_names = c.execute(sql_fields).fetchall()
        field_names = [(i[1], i[2]) for i in field_names]
        return field_names

    @staticmethod
    def get_schema(conn, table):
        """Get schema from table in Transfer Database

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param table: Name of table
        :type table: str
        :rtype: list
        """

        c = conn.cursor()
        sql_schema = ("SELECT sql FROM sqlite_master " +
                      f"WHERE type='table' and name='{table}'")
        schema = c.execute(sql_schema).fetchall()
        return schema

    def check_duplicates(self, conn):
        """Search for duplicate VA records.

        This method searches for duplicate VA records in ODK Briefcase export
        file and the Transfer DB.  If duplicates are found, a warning message
        is logged to the EventLog table in the Transfer database and the
        duplicate records are removed from the ODK Briefcase export file.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :raises: DatabaseConnectionError, PipelineError
        """

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        c = conn.cursor()
        odk_bc_export_path = os.path.join(
            self.working_directory, "ODKFiles", "odkBCExportNew.csv"
        )
        df_odk = read_csv(odk_bc_export_path)
        df_odk_id = df_odk["meta-instanceID"]

        sql = "SELECT id FROM VA_Storage"
        c.execute(sql)
        va_ids = c.fetchall()
        va_ids_list = [j for i in va_ids for j in i]
        va_duplicates = set(df_odk_id).intersection(set(va_ids_list))
        time_fmt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        if len(va_duplicates) > 0:
            n_duplicates = len(va_duplicates)
            sql_xfer_db = (
                "INSERT INTO EventLog "
                "(eventDesc, eventType, eventTime) VALUES (?, ?, ?)"
            )
            event_desc_part1 = [
                "Removing duplicate records from ODK Export with"
                + "ODK Meta-Instance ID: "
            ] * n_duplicates
            event_desc_part2 = list(va_duplicates)
            event_desc = [x + y for x, y in
                          zip(event_desc_part1, event_desc_part2)]
            event_type = ["Warning"] * n_duplicates
            event_time = [time_fmt] * n_duplicates
            par = list(zip(event_desc, event_type, event_time))
            c.executemany(sql_xfer_db, par)
            conn.commit()
            df_no_duplicates = df_odk[
                ~df_odk["meta-instanceID"].isin(list(va_duplicates))]
            try:
                df_no_duplicates.to_csv(odk_bc_export_path, index=False)
            except (PermissionError, OSError) as exc:
                raise PipelineError(
                    "Error trying to create new CSV file after " +
                    "removing duplicate records in ODK Export"
                ) from exc

    def config_openva(self, conn, algorithm):
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
        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param algorithm: VA algorithm used by R package openVA
        :type algorithm: str
        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        if algorithm == "InterVA":
            settings_interva = self._config_interva(conn)
            return settings_interva
        elif algorithm == "InSilicoVA":
            settings_insilicova = self._config_insilicova(conn)
            return settings_insilicova
        elif algorithm == "SmartVA":
            settings_smartva = self._config_smartva(conn)
            return settings_smartva
        else:
            raise PipelineConfigurationError(
                "Not an acceptable parameter for 'algorithm'."
            )

    @staticmethod
    def _config_interva(conn):
        """Query OpenVA configuration settings from database.

        This method is called by config_openva when the VA algorithm is either
        InterVA4 or InterVA5.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        c = conn.cursor()

        try:
            sql_interva = "SELECT version, HIV, Malaria FROM InterVA_Conf;"
            query_interva = c.execute(sql_interva).fetchall()
        except sqlcipher.OperationalError as e:
            raise PipelineConfigurationError(
                "Problem in database table InterVA_Conf..." + str(e)
            )

        # Database Table: InterVA_Conf
        interva_version = query_interva[0][0]
        if interva_version not in ("4", "5"):
            raise OpenVAConfigurationError(
                "Problem in database: InterVA_Conf.version "
                "(valid options: '4' or '5')."
            )
        interva_hiv = query_interva[0][1]
        if interva_hiv not in ("v", "l", "h"):
            raise OpenVAConfigurationError(
                "Problem in database: InterVA_Conf.HIV "
                "(valid options: 'v', 'l', or 'h')."
            )
        interva_malaria = query_interva[0][2]
        if interva_malaria not in ("v", "l", "h"):
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
        except sqlcipher.OperationalError as e:
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

    @staticmethod
    def _config_insilicova(conn):
        """Query OpenVA configuration settings from database.

        This method is called by config_openva when the VA algorithm is
        InSilicoVA.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        c = conn.cursor()

        # Database Table: InSilicoVA_Conf
        try:
            sql_insilicova = "SELECT data_type, Nsim FROM InSilicoVA_Conf;"
            query_insilicova = c.execute(sql_insilicova).fetchall()
        except sqlcipher.OperationalError as e:
            raise PipelineConfigurationError(
                "Problem in database table InSilicoVA_Conf..." + str(e)
            )

        insilicova_data_type = query_insilicova[0][0]
        if insilicova_data_type not in ("WHO2012", "WHO2016"):
            raise OpenVAConfigurationError(
                "Problem in database: InSilicoVA_Conf.data_type "
                "(valid options: 'WHO2012' or 'WHO2016')."
            )
        insilicova_nsim = query_insilicova[0][1]
        if insilicova_nsim in ("", None):
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
        except sqlcipher.OperationalError as e:
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

    @staticmethod
    def _config_smartva(conn):
        """Query OpenVA configuration settings from database.

        This method is called by config_openva when the VA algorithm is
        SmartVA.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :returns: Contains all parameters needed for
          OpenVA.setAlgorithmParameters().
        :rtype: (named) tuple
        :raises: OpenVAConfigurationError
        """

        c = conn.cursor()

        try:
            sql_smartva = (
                "SELECT country, hiv, malaria, hce, freetext, "
                "figures, language FROM SmartVA_Conf;"
            )
            query_smartva = c.execute(sql_smartva).fetchall()
        except sqlcipher.OperationalError as e:
            raise PipelineConfigurationError(
                "Problem in database table SmartVA_Conf..." + str(e)
            )

        try:
            sql_country_list = "SELECT abbrev FROM SmartVA_Country;"
            query_country_list = c.execute(sql_country_list).fetchall()
        except sqlcipher.OperationalError as e:
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

    @staticmethod
    def config_dhis(conn, algorithm):
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

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :param algorithm: VA algorithm used by R package openVA
        :type algorithm: str
        :returns: First item contains all parameters for
          :meth:`DHIS.connect() <openva_pipeline.dhis.DHIS.connect>`,
          and the second item contains the causes of death used by the
          VA Program (in DHIS2)
        :rtype: list [named tuple, dict]
        :raises: DHISConfigurationError
        """
        c = conn.cursor()
        try:
            sql_dhis = (
                "SELECT dhisURL, dhisUser, dhisPassword, dhisOrgUnit " 
                "FROM DHIS_Conf;"
            )
            query_dhis = c.execute(sql_dhis).fetchall()
        except sqlcipher.OperationalError as e:
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
        except sqlcipher.OperationalError as e:
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

        nt_dhis = namedtuple(
            "nt_dhis", ["dhis_url", "dhis_user",
                        "dhis_password", "dhis_org_unit"]
        )
        settings_dhis = nt_dhis(dhis_url, dhis_user, dhis_password,
                                dhis_org_unit)

        return [settings_dhis, dhis_cod_codes]

    def store_va(self, conn):
        """Store VA records in Transfer database.

        This method is intended to be used in conjunction with the
        :class:`DHIS <openva_pipeline.dhis.DHIS>` class, which prepares the
        records into the proper format for storage in the Transfer database.

        :param conn: A connection to the Transfer Database (e.g. the object
          returned from :meth:`TransferDB.connect_db() <connect_db>`.)
        :type conn: sqlite3 Connection object
        :raises: PipelineError, DatabaseConnectionError
        """

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        c = conn.cursor()
        new_storage_path = os.path.join(
            self.working_directory, "OpenVAFiles", "newStorage.csv"
        )
        df_new_storage = read_csv(new_storage_path)
        time_fmt = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        try:
            for row in df_new_storage.itertuples():
                xfer_db_id = row[1]
                n_elements = len(row) - 1
                xfer_db_outcome = row[n_elements]
                va_data = row[1], row[8:(n_elements - 1)]
                va_data_flat = tuple(
                    [y for x in va_data for y in
                     (x if isinstance(x, tuple) else (x,))]
                )
                xfer_db_record = dumps(va_data_flat)
                sql_xfer_db = (
                    "INSERT INTO VA_Storage "
                    "(id, outcome, record, dateEntered) "
                    "VALUES (?, ?, ?, ?)"
                )
                par = [xfer_db_id,
                       xfer_db_outcome,
                       sqlite3.Binary(xfer_db_record),
                       time_fmt]
                c.execute(sql_xfer_db, par)
            conn.commit()
        except sqlcipher.OperationalError as e:
            raise DatabaseConnectionError(
                "Problem storing VA record to Transfer DB..." + str(e))

    def make_pipeline_dirs(self):
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

    def clean_odk(self):
        """Remove ODK Briefcase Export files."""

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        odk_export_new_path = os.path.join(
            self.working_directory, "ODKFiles", "odkBCExportNew.csv"
        )
        odk_export_prev_path = os.path.join(
            self.working_directory, "ODKFiles", "odkBCExportPrev.csv"
        )
        if os.path.isfile(odk_export_new_path):
            os.remove(odk_export_new_path)
        if os.path.isfile(odk_export_prev_path):
            os.remove(odk_export_prev_path)

    def clean_openva(self):
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

    def clean_dhis(self):
        """Remove DHIS2 blob files."""

        if self.working_directory is None:
            raise PipelineError("Need to run config_pipeline.")
        rmtree(os.path.join(self.working_directory, "DHIS", "blobs"))
