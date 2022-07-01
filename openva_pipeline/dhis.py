"""
openva_pipeline.dhis
--------------------

This module posts VA records with assigned causes of death to a DHIS server.
"""

import requests
from pandas import read_csv
from pandas import isnull
from math import isnan
import sqlite3
import os
import csv
import datetime
import json
import re

from .exceptions import DHISError


class API(object):
    """This class provides methods for interacting with the DHIS2 API.

    This class is called by an instance of the :class:`DHIS <DHIS>` to retrieve
    information from and post verbal autopsy records (and results) to a
    DHIS2 server that has the Verbal Autopsy program installed.

    :param dhis_url: Web address for DHIS2 server (e.g. "play.dhis2.org/demo").
    :type dhis_url: string
    :param dhis_user: Username for DHIS2 account.
    :type dhis_user: string
    :param dhis_password: Password for DHIS2 account.
    :type dhis_password: string
    :raises: DHISError
    """

    def __init__(self, dhis_url, dhis_user, dhis_password):

        if "/api" in dhis_url:
            raise DHISError(
                "Please do not specify /api/ in the server argument: \
              e.g. --server=play.dhis2.org/demo"
            )
        if dhis_url.startswith("localhost") or dhis_url.startswith("127.0.0.1"):
            dhis_url = "http://{}".format(dhis_url)
        elif dhis_url.startswith("http://"):
            dhis_url = dhis_url
        elif not dhis_url.startswith("https://"):
            dhis_url = "https://{}".format(dhis_url)
        self.auth = (dhis_user, dhis_password)
        self.url = "{}/api".format(dhis_url)

    def get(self, endpoint, params=None):
        """GET method for DHIS2 API.

        :rtype: dict
        """

        url = "{}/{}.json".format(self.url, endpoint)
        if not params:
            params = {}
        params["paging"] = False
        try:
            r = requests.get(url=url, params=params, auth=self.auth)
            if r.status_code != 200:
                raise DHISError("HTTP Code: {} and ({})".format(r.status_code,
                                                                r.text))
            else:
                return r.json()
        except requests.RequestException:
            raise DHISError(str(requests.RequestException))

    def post(self, endpoint, data):
        """POST method for DHIS2 API.

        :rtype: dict
        """

        url = "{}/{}.json".format(self.url, endpoint)
        try:
            r = requests.post(url=url, json=data, auth=self.auth)
            if r.status_code not in range(200, 206):
                raise DHISError(
                    "Problem with API.post..."
                    + "HTTP Code: {}...".format(r.status_code)
                    + str(r.text)
                )
            else:
                return r.json()
        except requests.RequestException:
            raise DHISError("Problem with API.post..." +
                            str(requests.RequestException))

    def post_blob(self, db_file):
        """Post file to DHIS2 and return created UID for that file

        :rtype: str
        """

        url = "{}/fileResources".format(self.url)
        with open(db_file, "rb") as file_name:
            files = {"file": (db_file, file_name, "application/x-sqlite3",
                              {"Expires": "0"})}
            try:
                r = requests.post(url, files=files, auth=self.auth)
                if r.status_code not in (200, 202):
                    raise DHISError(
                        "Problem with API.post_blob..."
                        + "HTTP Code: {}...".format(r.status_code)
                        + str(r.text)
                    )
                else:
                    response = r.json()
                    file_id = response["response"]["fileResource"]["id"]
                    return file_id
            except requests.RequestException:
                raise DHISError(
                    "Problem with API.post_blob..." +
                    str(requests.RequestException)
                )

    def put(self, endpoint, data):
        """PUT method for DHIS2 API.

        :rtype: dict
        """

        url = "{}/{}.json".format(self.url, endpoint)
        try:
            r = requests.put(url=url, json=data, auth=self.auth)
            if r.status_code not in range(200, 206):
                raise DHISError(
                    "Problem with API.post..."
                    + "HTTP Code: {}...".format(r.status_code)
                    + str(r.text)
                )
            else:
                return r.json()
        except requests.RequestException:
            raise DHISError("Problem with API.put..." +
                            str(requests.RequestException))


class VerbalAutopsyEvent(object):
    """Create DHIS2 event + a BLOB file resource

    :param va_id: UID for verbal autopsy record
      (used as a DHIS2 data element)
    :type va_id: string
    :param program: UID of the DHIS2's Verbal Autopsy program
    :type program: string
    :param dhis_org_unit: UID for the DHIS2 Organization Unit where the
      event (death) should be registered.
    :type dhis_org_unit: string
    :param event_date: Date of death with YYYY-MM-DD format
    :type event_date: date
    :param sex: Sex of the deceased (used as a DHIS2 data element).
      Possible values must fit to an option in the VA Program's "Sex"
      optionSet: female, male, missing, unknown).  If SmartVA is used
      to assign cause of death, then sex is an integer with 1 = male
      and 2 = female).
    :type sex: string or integer
    :param dob: Date of birth of the deceased with YYYY-MM-DD format
      (used as a DHIS2 data element)
    :type dob: date
    :param age: Age (in years) at time of death
    :type age: integer
    :param cod_code: Coded cause of death (must fit to an option in the
      VA Program's "CoD codes" optionSet.
    :type cod_code: string
    :param algorithm_metadata: Code for how the CoD was obtained (must
      fit in VA Program's "Algorithm Metadata" optionSet.
    :type algorithm_metadata: string
    :param odk_id: UID for the VA record assigned by the ODK Aggregate server
    :type odk_id: string
    :param file_id: UID for the blob file (containing the VA data and
      results) posted to (and assigned by) DHIS2 server.
    :type file_id: string
    """

    def __init__(
        self,
        va_id,
        program,
        dhis_org_unit,
        event_date,
        sex,
        dob,
        age,
        cod_code,
        algorithm_metadata,
        odk_id,
        file_id,
    ):
        self.va_id = va_id
        self.program = program
        self.dhis_org_unit = dhis_org_unit
        self.event_date = event_date
        self.sex = sex
        self.dob = dob.strftime("%Y-%m-%d")
        self.age = age
        self.cod_code = cod_code
        self.algorithm_metadata = algorithm_metadata
        self.odk_id = odk_id
        self.file_id = file_id

    def format_se_to_dhis2(self, dhis_user, dhis_org_unit):
        """
        Format object to DHIS2 (single event) compatible event for DHIS2 API

        :param dhis_user: DHIS2 username for account posting the event
        :param dhis_org_unit: code for DHIS2 organization unit where the death
          will be posted
        :returns: DHIS2 event
        :rtype: dict
        """
        data_values = [
            {"dataElement": "htm6PixLJNy", "value": self.va_id},
            {"dataElement": "hi7qRC4SMMk", "value": self.sex},
            {"dataElement": "mwSaVq64k7j", "value": self.dob},
            {"dataElement": "F4XGdOBvWww", "value": self.cod_code},
            {"dataElement": "wiJviUqN1io", "value": self.algorithm_metadata},
            {"dataElement": "LwXZ2dZmJb0", "value": self.odk_id},
            {"dataElement": "XLHIBoLtjGt", "value": self.file_id},
        ]
        if self.age != "MISSING":
            data_values.append({"dataElement": "oPAg4MA0880",
                                "value": self.age})
        formatted_event_date = self.event_date.strftime("%Y-%m-%d")
        event = {
            "program": self.program,
            "orgUnit": dhis_org_unit,
            "eventDate": formatted_event_date,
            "status": "COMPLETED",
            "storedBy": dhis_user,
            "dataValues": data_values,
        }
        return event

    def format_tea_to_dhis2(self, dhis_user, dhis_org_unit, tei_id=None):
        """
        Format object to DHIS2 (tracker) compatible event for DHIS2 API

        :param dhis_user: DHIS2 username for account posting the event
        :param dhis_org_unit: code for DHIS2 organization unit where the death
          will be posted
        :param tei_id: ID for the registered tracked entity instance (None if
          the tei has not been registered)
        :returns: DHIS2 event
        :rtype: dict
        """
        data_values = [
            {"dataElement": "htm6PixLJNy", "value": self.va_id},
            {"dataElement": "F4XGdOBvWww", "value": self.cod_code},
            {"dataElement": "wiJviUqN1io", "value": self.algorithm_metadata},
            {"dataElement": "LwXZ2dZmJb0", "value": self.odk_id},
            {"dataElement": "XLHIBoLtjGt", "value": self.file_id},
        ]
        if self.age != "MISSING":
            data_values.append({"dataElement": "oPAg4MA0880",
                                "value": self.age})
        attributes = [
            {"attribute": "XSFOyybvYJ9", "value": self.sex},
            {"attribute": "P1xsdeFzhCb", "value": self.dob}
        ]
        formatted_event_date = datetime.datetime.strftime(self.event_date,
                                                          "%Y-%m-%d")
        events = [{
            "program": self.program,
            "orgUnit": dhis_org_unit,
            "eventDate": formatted_event_date,
            "status": "COMPLETED",
            "programStage": "OiZFUyH5KnN",
            "storedBy": dhis_user,
            "dataValues": data_values,
        }]
        enrollments = [{
            "orgUnit": dhis_org_unit,
            "program": self.program,
            "events": events
        }]

        tracked_entity_instance = {
            "trackedEntityType": "j7AIUZGpUxF",
            "orgUnit": dhis_org_unit,
            "attributes": attributes,
            "enrollments": enrollments
        }
        if tei_id is not None:
            tracked_entity_instance["trackedEntityInstance"] = tei_id

        return tracked_entity_instance

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__)


def create_db(file_name, eva_list):
    """
    Create a SQLite database with VA data + COD

    :param file_name: Name (including path) of sqlite3 db file (blob) that
      will be posted to DHIS2
    :type file_name: str
    :param eva_list: Event-Value-Attribute data structure with verbal autopsy
      data, cause of death result, and VA metadata.
    :type eva_list: list
    :rtype: None
    """
    conn = sqlite3.connect(file_name)
    with conn:
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE vaRecord(ID INT, Attribute TEXT, Value TEXT)")
        cur.executemany("INSERT INTO vaRecord VALUES (?,?,?)", eva_list)
    conn.close()


def get_cod_code(my_dict, search_for):
    """Return COD label expected by (DHIS2) VA Program.

    :param my_dict: Dictionary to search
    :type my_dict: dict
    :param search_for: Cause of Death label returned by openVA.
    :type search_for: string
    :rtype: str
    """
    for i in range(len(my_dict.keys())):
        match = re.search(re.escape(search_for), list(my_dict.keys())[i])
        if match:
            return list(my_dict.values())[i]


def find_key_value(key, my_dict):
    """
    Return a key's value in a nested dictionary.
    """
    if key in my_dict:
        yield my_dict[key]
    for k in my_dict:
        if isinstance(my_dict[k], list):
            for i in my_dict[k]:
                for j in find_key_value(key, i):
                    yield j


class DHIS:
    """Class for transferring VA records (with assigned CODs) to the DHIS2.

    This class includes methods for importing VA results (i.e. assigned causes
    of death from openVA or SmartVA) as CSV files, connecting to a DHIS2 server
    with the Verbal Autopsy Program, and posting the results to the DHIS2
    server and/or the local Transfer database.

    :param dhis_args: Contains parameter values for connected to DHIS2, as
      returned by transferDB.config_dhis().
    :type dhis_args: (named) tuple
    :param working_directory: Working directory for the openVA Pipeline
    :type working_directory: string
    :raises: DHISError
    """

    def __init__(self, dhis_args, working_directory):

        self.dhis_url = dhis_args[0].dhis_url
        self.dhis_user = dhis_args[0].dhis_user
        self.dhis_password = dhis_args[0].dhis_password
        self.dhis_org_unit = dhis_args[0].dhis_org_unit
        self.dhis_cod_codes = dhis_args[1]
        self.dir_dhis = os.path.join(working_directory, "DHIS")
        self.dir_openva = os.path.join(working_directory, "OpenVAFiles")
        self.va_program_uid = None
        self.n_posted_records = 0
        self.post_to_tracker = False

        dhis_path = os.path.join(working_directory, "DHIS")

        try:
            if not os.path.isdir(dhis_path):
                os.makedirs(dhis_path)
        except (PermissionError, OSError) as exc:
            raise DHISError("Unable to create directory" + str(exc)) from exc

    def connect(self):
        """Setup connection to DHIS2 server.

        This creates a connection to DHIS2's VA Program ID
        by creating an instance of :class:`API <API>`.  This method
        also checks that the VA Program and the organization unit
        can both be found on the DHIS2 server.  The configuration
        settings for connecting to the DHIS2 (e.g., URL, username,
        password, etc.) are taken from the arguments passed to this
        method's class :class:`DHIS <DHIS>` (these settings can be
        created using the method
        :meth:`Pipeline.config <openva_pipeline.pipeline.Pipeline.config>`).

        :returns: A class instance for interacting with the DHIS2 API.
        :rtype: Instance of the :class:`API <API>` class
        """

        try:
            api_dhis = API(self.dhis_url, self.dhis_user, self.dhis_password)
        except requests.RequestException as exc:
            raise DHISError(str(exc)) from exc

        va_programs = api_dhis.get(
            "programs", params={"filter": "name:like:Verbal Autopsy"}
        ).get("programs")
        if len(va_programs) == 0:
            raise DHISError("No Verbal Autopsy Program found.")
        if len(va_programs) > 1:
            raise DHISError("More than one Verbal Autopsy Program found.")
        self.va_program_uid = va_programs[0].get("id")

        return api_dhis

    def post_va(self, api_dhis):
        """Post VA records to DHIS.

        This method reads in a CSV file ("entity_attributes_value.csv") with
        cause of death results (from openVA) then formats events and posts
        them to a VA Program (installed on DHIS2 server).

        :param api_dhis: A class instance for interacting with the DHIS2 API
          created by the method :meth:`DHIS.connect <connect>`
        :type api_dhis: Instance of the :class:`API <API>` class
        :returns: Log information received after posting events to the VA
          Program on a DHIS2 server (see :meth:`API.post <API.post>`).
        :rtype: dict
        :raises: DHISError
        """

        eva_path = os.path.join(self.dir_openva, "entity_attribute_value.csv")
        if not os.path.isfile(eva_path):
            raise DHISError("Missing: " + eva_path)
        record_storage_path = os.path.join(self.dir_openva,
                                           "record_storage.csv")
        if not os.path.isfile(record_storage_path):
            raise DHISError("Missing: " + record_storage_path)
        new_storage_path = os.path.join(self.dir_openva, "new_storage.csv")

        blob_path = os.path.join(self.dir_dhis, "blobs")
        try:
            if not os.path.isdir(blob_path):
                os.makedirs(blob_path)
        except OSError as exc:
            raise DHISError(
                "Unable to create directory for DHIS blobs.") from exc

        df_dhis = read_csv(eva_path)
        grouped = df_dhis.groupby(["ID"])
        df_record_storage = read_csv(record_storage_path)
        server_org_units = api_dhis.get(
            "organisationUnits").get("organisationUnits")
        server_org_unit_names = [i['displayName'] for i in server_org_units]
        server_org_unit_ids = [i['id'] for i in server_org_units]
        server_top_org_unit = api_dhis.get(
            "organisationUnits",
            params={"filter": "level:eq:1"})["organisationUnits"][0]
        server_top_org_unit_id = server_top_org_unit["id"]
        tea = api_dhis.get(
            "trackedEntityAttributes")["trackedEntityAttributes"]
        tea_names = [i['displayName'] for i in tea]

        events = []
        export = {}
        if "VA-02-Sex" in tea_names:
            self.post_to_tracker = True
            tracked_entity_instances = []

        with open(new_storage_path, "w", newline="") as csv_out:
            writer = csv.writer(csv_out)

            header = list(df_record_storage)
            header.extend(["dhisVerbalAutopsyID", "pipelineOutcome"])
            writer.writerow(header)

            # this depends on openVA vs SmartVA
            for i in df_record_storage.itertuples(index=False):
                row = list(i)

                if row[5] != "MISSING" and row[5] is not None:

                    va_id = str(row[0])
                    blob_file = "{}.db".format(os.path.join(self.dir_dhis,
                                                            "blobs",
                                                            va_id))
                    blob_record = grouped.get_group(str(row[0]))
                    blob_eva = blob_record.values.tolist()

                    try:
                        create_db(blob_file, blob_eva)
                    except sqlite3.OperationalError as exc:
                        raise DHISError("Unable to create blob.") from exc
                    try:
                        file_id = api_dhis.post_blob(blob_file)
                    except requests.RequestException as exc:
                        raise DHISError("Unable to post blob to DHIS..." +
                                        str(exc)) from exc

                    algorithm = row[7].split("|")[0]
                    if algorithm == "SmartVA":
                        if row[1] in ["1", "1.0", 1, 1.0]:
                            sex = "male"
                        elif row[1] in ["2", "2.0", 2, 2.0]:
                            sex = "female"
                        elif row[1] in ["8", "8.0", 8, 8.0]:
                            sex = "don't know"
                        else:
                            sex = "refused to answer"
                    else:
                        sex = row[1].lower()
                    if isnull(row[2]):
                        dob = datetime.date(9999, 9, 9)
                    else:
                        dob_temp = datetime.datetime.strptime(row[2],
                                                              "%Y-%m-%d")
                        dob = datetime.date(dob_temp.year,
                                            dob_temp.month,
                                            dob_temp.day)
                    if isnull(row[3]):
                        event_date = datetime.date(9999, 9, 9)
                    else:
                        dod = datetime.datetime.strptime(row[3], "%Y-%m-%d")
                        event_date = datetime.date(dod.year,
                                                   dod.month,
                                                   dod.day)
                    if type(row[4]) == int:
                        age = row[4]
                    elif type(row[4]) == float and not isnan(row[4]):
                        age = int(row[4])
                    else:
                        age = "MISSING"
                    if row[5] == "Undetermined":
                        cod_code = "99"
                    else:
                        cod_code = get_cod_code(self.dhis_cod_codes, row[5])

                    row_dict = i._asdict()
                    ou_keys = [key for key, val in row_dict.items()
                               if ('org_unit_col' in key) and
                               isinstance(val, str)]
                    ou_keys.sort()
                    death_org_unit = row_dict[ou_keys[-1]]
                    #death_org_unit = row[6]
                    if death_org_unit in server_org_unit_names:
                        org_unit_index = server_org_unit_names.index(
                            death_org_unit)
                        dhis_org_unit = server_org_unit_ids[org_unit_index]
                    elif death_org_unit in server_org_unit_ids:
                        dhis_org_unit = death_org_unit
                    else:
                        # TODO: add parameter for what to do when can't find
                        # DHIS org unit with options post to root or don't post
                        # (note user may not have permission to post here!!!)
                        dhis_org_unit = server_top_org_unit_id

                    #algorithm_metadata_code = row[7]
                    algorithm_metadata_code = row_dict['metadataCode']
                    #odk_id = row[8]
                    odk_id = row_dict['odkMetaInstanceID']

                    e = VerbalAutopsyEvent(
                        va_id,
                        self.va_program_uid,
                        dhis_org_unit,
                        event_date,
                        sex,
                        dob,
                        age,
                        cod_code,
                        algorithm_metadata_code,
                        odk_id,
                        file_id,
                    )

                    if self.post_to_tracker:
                        # tei_dict = api_dhis.get("trackedEntityInstances",
                        #                         params={"ou": dhis_org_unit})
                        tracked_entity_instances.append(
                            e.format_tea_to_dhis2(self.dhis_user,
                                                  dhis_org_unit))
                    else:
                        events.append(e.format_se_to_dhis2(self.dhis_user,
                                                           dhis_org_unit))
                    row.extend([va_id, "Pushing to DHIS2"])
                    writer.writerow(row)
                else:
                    row.extend(["", "No CoD Assigned"])
                    writer.writerow(row)
        try:
            if self.post_to_tracker:
                export["trackedEntityInstances"] = tracked_entity_instances
                log = api_dhis.post("trackedEntityInstances", data=export)
            else:
                export["events"] = events
                log = api_dhis.post("events", data=export)
        except requests.RequestException as exc:
            raise DHISError("Unable to post events to DHIS2..." + str(exc))
        self.n_posted_records = len(log["response"]["importSummaries"])
        return log

    def verify_post(self, post_log, api_dhis):
        """Verify that VA records were posted to DHIS2 server.

        :param post_log: Log information retrieved after posting events to
          a VA Program on a DHIS2 server; this is the return object from
          :meth:`DHIS.post_va <post_va>`.
        :type post_log: dictionary
        :param api_dhis: A class instance for interacting with the DHIS2 API
          created by the method :meth:`DHIS.connect <connect>`
        :type api_dhis: Instance of the :class:`API <API>` class
        :raises: DHISError
        """

        va_references = list(find_key_value("reference",
                                            my_dict=post_log["response"]))
        try:
            df_new_storage = read_csv(self.dir_openva + "/new_storage.csv")
        except FileNotFoundError:
            raise DHISError(
                "Problem with DHIS.verify_post...Can't find file "
                + self.dir_openva
                + "/new_storage.csv"
            )
        try:
            for va_reference in va_references:
                posted_data_values = api_dhis.get(
                    "events".format(va_reference)).get("dataValues")
                posted_va_id_index = next(
                    (
                        index
                        for (index, d) in enumerate(posted_data_values)
                        if d["dataElement"] == "htm6PixLJNy"
                    ),
                    None,
                )
                posted_va_id = posted_data_values[posted_va_id_index]["value"]
                row_va_id = df_new_storage[
                                "dhisVerbalAutopsyID"] == posted_va_id
                df_new_storage.loc[row_va_id,
                                   "pipelineOutcome"] = "Pushed to DHIS2"
            df_new_storage.to_csv(self.dir_openva + "/new_storage.csv",
                                  index=False)
        except (requests.RequestException, ) as exc:
            raise DHISError(
                "Problem verifying posted records with DHIS.post_va..." +
                str(exc)) from exc

    def verify_tei_post(self, post_log, api_dhis):
        """Verify that VA tracked entity instances (tei) were posted to
        DHIS2 server.

        :param post_log: Log information retrieved after posting events to
          a VA Program on a DHIS2 server; this is the return object from
          :meth:`DHIS.post_va <post_va>`.
        :type post_log: dictionary
        :param api_dhis: A class instance for interacting with the DHIS2 API
          created by the method :meth:`DHIS.connect <connect>`
        :type api_dhis: Instance of the :class:`API <API>` class
        :raises: DHISError
        """

        va_references = list(find_key_value("reference",
                                            my_dict=post_log["response"]))
        try:
            df_new_storage = read_csv(self.dir_openva + "/new_storage.csv")
        except FileNotFoundError:
            raise DHISError(
                "Problem with DHIS.verify_post...Can't find file "
                + self.dir_openva
                + "/new_storage.csv"
            )
        try:
            for va_reference in va_references:
                params = {"trackedEntityInstance": va_reference}
                posted_event = api_dhis.get("events",
                                            params=params)['events'][0]
                posted_data_values = posted_event['dataValues']
                posted_va_id = [d["value"] for d in posted_data_values
                                if d["dataElement"] == "htm6PixLJNy"]
                row_va_id = df_new_storage[
                                "dhisVerbalAutopsyID"] == posted_va_id[0]
                df_new_storage.loc[row_va_id,
                                   "pipelineOutcome"] = "Pushed to DHIS2"
            df_new_storage.to_csv(self.dir_openva + "/new_storage.csv",
                                  index=False)
        except (requests.RequestException, ) as exc:
            raise DHISError(
                "Problem verifying posted records with DHIS.post_va..." +
                str(exc)) from exc
