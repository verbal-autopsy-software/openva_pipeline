"""
openva_pipeline.dhis
--------------------

This module posts VA records with assigned causes of death to a DHIS server.
"""

import requests
from pandas import read_csv, DataFrame
from pandas import isnull
from math import isnan
import sqlite3
import os
import csv
import datetime
import json
import re
from collections import OrderedDict
from typing import Union, Dict

import openva_pipeline
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
        except requests.RequestException as exc:
            raise DHISError(str(exc))

    def post(self, endpoint, data, params=None):
        """POST method for DHIS2 API.

        :rtype: dict
        """

        url = "{}/{}.json".format(self.url, endpoint)
        try:
            r = requests.post(url=url, json=data, params=params, auth=self.auth)
            if r.status_code not in range(200, 206):
                raise DHISError(
                    "Problem with API.post..."
                    + "HTTP Code: {}...".format(r.status_code)
                    + str(r.text)
                )
            else:
                return r.json()
        except requests.RequestException as exc:
            raise DHISError("Problem with API.post..." +
                            str(exc))

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
            except requests.RequestException as exc:
                raise DHISError(
                    "Problem with API.post_blob..." +
                    str(exc)
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
                    "Problem with API.put..."
                    + "HTTP Code: {}...".format(r.status_code)
                    + str(r.text)
                )
            else:
                return r.json()
        except requests.RequestException as exc:
            raise DHISError("Problem with API.put..." +
                            str(exc))

    def _delete_event(self, uid):
        """DELETE an event (without registration)."""

        url = "{}/events/{}".format(self.url, uid)
        try:
            r = requests.delete(url=url, auth=self.auth)
            if r.status_code not in range(200, 206):
                raise DHISError(
                    "Problem with API.delete..."
                    + "HTTP Code: {}...".format(r.status_code)
                    + str(r.text)
                )
        except requests.RequestException as exc:
            raise DHISError("Problem with API.delete..." +
                            str(exc))


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
            # {"dataElement": "mwSaVq64k7j", "value": self.dob},
            {"dataElement": "F4XGdOBvWww", "value": self.cod_code},
            {"dataElement": "wiJviUqN1io", "value": self.algorithm_metadata},
            {"dataElement": "LwXZ2dZmJb0", "value": self.odk_id},
            {"dataElement": "XLHIBoLtjGt", "value": self.file_id},
        ]
        if self.age != "MISSING":
            data_values.append({"dataElement": "oPAg4MA0880",
                                "value": self.age})
        if self.age != "9999-09-09":
            data_values.append({"dataElement": "mwSaVq64k7j",
                                "value": self.dob})
        event = {
            "program": self.program,
            "orgUnit": dhis_org_unit,
            # "eventDate": formatted_event_date,
            "status": "COMPLETED",
            "storedBy": dhis_user,
            "dataValues": data_values,
        }
        if self.event_date != datetime.date(9999, 9, 9):
            formatted_event_date = self.event_date.strftime("%Y-%m-%d")
            event["eventDate"] = formatted_event_date
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
            # {"attribute": "P1xsdeFzhCb", "value": self.dob}
        ]
        if self.dob != "9999-09-09":
            attributes.append({"attribute": "P1xsdeFzhCb",
                               "value": self.dob})
        events = [{
            "program": self.program,
            "orgUnit": dhis_org_unit,
            # "eventDate": formatted_event_date,
            "status": "COMPLETED",
            "programStage": "OiZFUyH5KnN",
            "storedBy": dhis_user,
            "dataValues": data_values,
        }]
        if self.event_date != datetime.date(9999, 9, 9):
            formatted_event_date = datetime.datetime.strftime(self.event_date,
                                                              "%Y-%m-%d")
            events[0]["eventDate"] = formatted_event_date
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
    if os.path.isfile(file_name):
        os.remove(file_name)
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


def _find_org_unit(find_ou: list, all_ou: list) -> str:
    """Find matching DHIS org unit.

    :param find_ou: Names of organisation units for death.
    :param all_ou: Names of organisation units in DHIS2 hierarchy.
    :rtype: str
    """

    n = len(find_ou)
    ou_matches = OrderedDict()
    for ou in range(n):
        try:
            ou_pattern = re.compile(
                "(^|\s)" + find_ou[ou].lower() + "(\s|$)")
        except re.error:
            ou_clean = find_ou[ou].replace("\\", "")
            ou_pattern = re.compile(
                "(^|\s)" + ou_clean.lower() + "(\s|$)")
        ou_match = [i for i in all_ou if re.search(ou_pattern, i.lower())]
        if ou_match:
            ou_matches[f"level_{ou}"] = ou_match

    if not ou_matches:
        return "No match found"

    current_ou = ou_matches.popitem(last=True)[1]
    if len(current_ou) == 1:
        return current_ou[0]

    while ou_matches:
        next_ou = ou_matches.popitem(last=True)[1]
        if set(current_ou) & set(next_ou):
            current_ou = list(set(current_ou) & set(next_ou))
            if len(current_ou) == 1:
                return current_ou[0]
        # elif return no match?
    return "No match found"


class DHIS:
    """Class for transferring VA records (with assigned CODs) to the DHIS2.

    This class includes methods for importing VA results (i.e. assigned causes
    of death from openVA or SmartVA) as CSV files, connecting to a DHIS2 server
    with the Verbal Autopsy Program, and posting the results to the DHIS2
    server and/or the local Transfer database.

    :param dhis_args: Contains parameter values for connected to DHIS2, as
      returned by transferDB.config_dhis().
    :type dhis_args: list of namedtuple and dictionary with COD codes
    :param working_directory: Working directory for the openVA Pipeline
    :type working_directory: string
    :raises: DHISError
    """

    def __init__(self, dhis_args, working_directory):

        self.dhis_url = dhis_args[0].dhis_url
        self.dhis_user = dhis_args[0].dhis_user
        self.dhis_password = dhis_args[0].dhis_password
        self.dhis_org_unit = dhis_args[0].dhis_org_unit
        self.dhis_post_root = dhis_args[0].dhis_post_root
        self.dhis_cod_codes = dhis_args[1]
        self.dir_dhis = os.path.join(working_directory, "DHIS")
        self.dir_openva = os.path.join(working_directory, "OpenVAFiles")
        self.va_program_uid = None
        self.n_posted_events = 0
        self.n_no_valid_org_unit = 0
        self.post_to_tracker = False
        self.api_dhis = None

        dhis_path = os.path.join(working_directory, "DHIS")

        try:
            if not os.path.isdir(dhis_path):
                os.makedirs(dhis_path)
        except (PermissionError, OSError) as exc:
            raise DHISError("Unable to create directory" + str(exc)) from exc

        self._connect()

    def _connect(self):
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
        """

        try:
            self.api_dhis = API(self.dhis_url,
                                self.dhis_user,
                                self.dhis_password)
        except requests.RequestException as exc:
            raise DHISError(str(exc)) from exc

        va_programs = self.api_dhis.get(
            "programs", params={"filter": "name:like:Verbal Autopsy"}
        ).get("programs")
        if len(va_programs) == 0:
            raise DHISError("No Verbal Autopsy Program found.")
        if len(va_programs) > 1:
            raise DHISError("More than one Verbal Autopsy Program found.")
        self.va_program_uid = va_programs[0].get("id")
        tea = self.api_dhis.get(
            "trackedEntityAttributes")["trackedEntityAttributes"]
        tea_names = [i['displayName'] for i in tea]
        if "VA-02-Sex" in tea_names:
            self.post_to_tracker = True

    def _get_org_units(self,
                       level: Union[int, None] = None,
                       va_program: bool = True) -> dict:
        """Get DHIS organisation unit IDs and display names.
        :param va_program: Indicator for returning only organisation units
        in the DHIS VA Program (as opposed to all organisation units).
        :type va_program: bool
        :returns: display names and IDs of DHIS organisation units.
        :rtype: dict
        """

        params = None
        if level and isinstance(level, int):
            params = {"filter": f"level:eq:{level}"}

        all_ou = self.api_dhis.get(
            "organisationUnits", params=params).get("organisationUnits")
        if all_ou:
            ou_dict = {i.get("displayName"): i.get("id") for i in all_ou}
        else:
            ou_dict = {"No org units at level": level}
        if not va_program:
            return ou_dict
        else:
            va_ou = self.api_dhis.get(
                f"programs/{self.va_program_uid}",
                params={"fields": "organisationUnits"}).get("organisationUnits")
            va_uid = [i.get("id") for i in va_ou]
            va_ou_dict = {k: v for k, v in ou_dict.items() if v in va_uid}
            return va_ou_dict

    def post_va(self, xfer_db: openva_pipeline.transfer_db.TransferDB) -> Dict:
        """Post VA records to DHIS.

        This method reads in a CSV file ("entity_attributes_value.csv") with
        cause of death results (from openVA) then formats events and posts
        them to a VA Program (installed on DHIS2 server).

        :param xfer_db: Transfer Database instance
        :type xfer_db: openva_pipeline.transfer_db.TransferDB
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

        va_org_units = self._get_org_units(va_program=True)
        valid_org_unit_ids = list(va_org_units.values())
        valid_org_unit_names = list(va_org_units.keys())
        top_org_unit_id = None
        if self.dhis_post_root == "True":
            top_org_unit = self.api_dhis.get(
                "organisationUnits",
                params={"filter": "level:eq:1"})["organisationUnits"][0]
            if top_org_unit["id"] in valid_org_unit_ids:
                top_org_unit_id = top_org_unit["id"]

        events = []
        export = {}
        if self.post_to_tracker:
            tracked_entity_instances = []

        with open(new_storage_path, "w", newline="") as csv_out:
            writer = csv.writer(csv_out)

            header = list(df_record_storage)
            header.extend(["dhisVerbalAutopsyID",
                           "pipelineOutcome",
                           "dhis_org_unit"])
            writer.writerow(header)

            # this depends on openVA vs SmartVA
            for row_dict in df_record_storage.to_dict(orient="records"):
                if row_dict["cod"] and row_dict["cod"] != "MISSING":
                    va_id = str(row_dict["id"])
                    blob_file = "{}.db".format(os.path.join(self.dir_dhis,
                                                            "blobs",
                                                            va_id))
                    blob_record = grouped.get_group(va_id)
                    blob_eva = blob_record.values.tolist()

                    try:
                        create_db(blob_file, blob_eva)
                    except sqlite3.OperationalError as exc:
                        raise DHISError("Unable to create blob.") from exc
                    try:
                        file_id = self.api_dhis.post_blob(blob_file)
                    except requests.RequestException as exc:
                        raise DHISError("Unable to post blob to DHIS..." +
                                        str(exc)) from exc

                    algorithm = row_dict["metadataCode"].split("|")[0]
                    if algorithm == "SmartVA":
                        if row_dict["sex"] in ["1", "1.0", 1, 1.0]:
                            sex = "male"
                        elif row_dict["sex"] in ["2", "2.0", 2, 2.0]:
                            sex = "female"
                        elif row_dict["sex"] in ["8", "8.0", 8, 8.0]:
                            sex = "don't know"
                        else:
                            sex = "refused to answer"
                    else:
                        sex = row_dict["sex"].lower()
                    if isnull(row_dict["dob"]):
                        dob = datetime.date(9999, 9, 9)
                    else:
                        dob_temp = datetime.datetime.strptime(row_dict["dob"],
                                                              "%Y-%m-%d")
                        dob = datetime.date(dob_temp.year,
                                            dob_temp.month,
                                            dob_temp.day)
                    if isnull(row_dict["dod"]):
                        event_date = datetime.date(9999, 9, 9)
                    else:
                        dod = datetime.datetime.strptime(row_dict["dod"],
                                                         "%Y-%m-%d")
                        event_date = datetime.date(dod.year,
                                                   dod.month,
                                                   dod.day)
                    if row_dict["age"] >= 0 and not isnan(row_dict["age"]):
                        age = int(row_dict["age"])
                    else:
                        age = "MISSING"
                    if row_dict["cod"] == "Undetermined":
                        cod_code = "99"
                    else:
                        cod_code = get_cod_code(self.dhis_cod_codes,
                                                row_dict["cod"])

                    org_unit_keys = [key for key, val in row_dict.items()
                                     if ("org_unit_col" in key) and
                                     (isinstance(val, str) or not isnull(val))]
                    org_unit_keys.sort()
                    death_org_unit_names = list(map(row_dict.get,
                                                    org_unit_keys))
                    if len(death_org_unit_names) == 1 and \
                            death_org_unit_names[0] in valid_org_unit_ids:
                        dhis_org_unit = death_org_unit_names[0]
                    else:
                        death_org_unit = _find_org_unit(
                            death_org_unit_names,
                            valid_org_unit_names)
                        if death_org_unit != "No match found":
                            org_unit_index = valid_org_unit_names.index(
                                death_org_unit)
                            dhis_org_unit = valid_org_unit_ids[org_unit_index]
                        # elif death_org_unit in valid_org_unit_ids:
                        #     dhis_org_unit = death_org_unit
                        else:
                            # Note: if dhis.post_to_root is False (or user does
                            # not have permission to post to root), then
                            # top_org_unit_id == None
                            dhis_org_unit = top_org_unit_id
                    algorithm_metadata_code = row_dict["metadataCode"]
                    odk_id = row_dict["odkMetaInstanceID"]

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

                    if dhis_org_unit is None:
                        # row_list = list(row_dict.values())
                        # row_list.extend(
                        #     [va_id,
                        #      f"Invalid DHIS2 org unit ({dhis_org_unit})"])
                        # writer.writerow(row_list)
                        data_ou_str = ",".join(death_org_unit_names)
                        xfer_db.store_no_ou_va(row_dict,
                                               blob_record,
                                               data_ou_str)
                        self.n_no_valid_org_unit += 1
                    else:
                        if self.post_to_tracker:
                            # tei_dict = self.api_dhis.get("trackedEntityInstances",
                            #                              params={"ou": dhis_org_unit})
                            tracked_entity_instances.append(
                                e.format_tea_to_dhis2(self.dhis_user,
                                                      dhis_org_unit))
                        else:
                            events.append(
                                e.format_se_to_dhis2(self.dhis_user,
                                                     dhis_org_unit))
                        row_list = list(row_dict.values())
                        row_list.extend([va_id,
                                         "Pushing to DHIS2",
                                         dhis_org_unit])
                        writer.writerow(row_list)
                else:
                    row_list = list(row_dict.values())
                    row_list.extend(["", "No CoD Assigned", dhis_org_unit])
                    writer.writerow(row_list)
        try:
            if self.post_to_tracker:
                export["trackedEntityInstances"] = tracked_entity_instances
                log = self.api_dhis.post("trackedEntityInstances", data=export)
            else:
                export["events"] = events
                log = self.api_dhis.post("events", data=export)
        except requests.RequestException as exc:
            raise DHISError("Unable to post events to DHIS2..." + str(exc))
        if self.post_to_tracker:
            tei_event_status = self._parse_tei_post_log(log)
            event_success = [k for k, v in tei_event_status.items()
                             if v.get("event_status") == "SUCCESS"]
            self.n_posted_events = len(event_success)
            # delete TEIs if event -> ERROR? (may not have permission)
            # package = {"trackedEntityInstances": [{
            #     "trackedEntityInstance": k}
            #     for k,v in tei_event_status.items()
            #     if v.get("status") == "ERROR"]}
            # self.api_dhis.post("trackedEntityInstances", data=package,
            #                    params={"strategy": "DELETE"})
        else:
            self.n_posted_events = len(log["response"]["importSummaries"])
        return log

    def post_single_va(self,
                       va_dict: Dict,
                       eav: DataFrame,
                       org_unit: str) -> Dict:
        """Post a single event to DHIS2

        :param va_dict: VA record with cause of death and metadata
        :type va_dict: dict
        :param eav: VA record in Entity-Attribute-Value format
        :type eav: DataFrame
        :param org_unit: DHIS2 organisation unit where the event will be
        posted
        :type org_unit: str
        """

        # TODO: figure out if you need to reset self.n_no_valid_org_unit to 0
        event = self._prep_va(va_dict=va_dict,
                              eav=eav,
                              org_unit=org_unit)
        if self.post_to_tracker:
            tracked_entity_instances = [event]
        else:
            events = [event]
        try:
            if self.post_to_tracker:
                export = {"trackedEntityInstances": tracked_entity_instances}
                log = self.api_dhis.post("trackedEntityInstances", data=export)
            else:
                export = {"events": events}
                log = self.api_dhis.post("events", data=export)
        except requests.RequestException as exc:
            raise DHISError("Unable to post events to DHIS2..." + str(exc))
        if self.post_to_tracker:
            tei_event_status = self._parse_tei_post_log(log)
            event_success = [k for k, v in tei_event_status.items()
                             if v.get("event_status") == "SUCCESS"]
            self.n_posted_events = len(event_success)
        else:
            self.n_posted_events = len(log["response"]["importSummaries"])
        return log

    def _prep_va(self,
                 va_dict: Dict,
                 eav: DataFrame,
                 org_unit: str) -> Dict:
        """Prepare VA data for posting to DHIS2

        :param va_dict: VA record with cause of death and metadata
        :type va_dict: dict
        :param eav: VA record in Entity-Value-Attribute format
        :type eav: DataFrame
        :param org_unit: DHIS2 organisation unit where the event will be
        posted
        :type org_unit: str
        """

        blob_path = os.path.join(self.dir_dhis, "blobs")
        try:
            if not os.path.isdir(blob_path):
                os.makedirs(blob_path)
        except OSError as exc:
            raise DHISError(
                "Unable to create directory for DHIS blobs.") from exc
        va_id = str(va_dict["id"])
        blob_file = "{}.db".format(os.path.join(self.dir_dhis,
                                                "blobs",
                                                va_id))
        # blob_record = grouped.get_group(va_id)
        # blob_eva = blob_record.values.tolist()
        blob_eva = eav.values.tolist()

        try:
            create_db(blob_file, blob_eva)
        except sqlite3.OperationalError as exc:
            raise DHISError("Unable to create blob.") from exc
        try:
            file_id = self.api_dhis.post_blob(blob_file)
        except requests.RequestException as exc:
            raise DHISError("Unable to post blob to DHIS..." +
                            str(exc)) from exc

        algorithm = va_dict["metadataCode"].split("|")[0]
        if algorithm == "SmartVA":
            if va_dict["sex"] in ["1", "1.0", 1, 1.0]:
                sex = "male"
            elif va_dict["sex"] in ["2", "2.0", 2, 2.0]:
                sex = "female"
            elif va_dict["sex"] in ["8", "8.0", 8, 8.0]:
                sex = "don't know"
            else:
                sex = "refused to answer"
        else:
            sex = va_dict["sex"].lower()
        if isnull(va_dict["dob"]):
            dob = datetime.date(9999, 9, 9)
        else:
            dob_temp = datetime.datetime.strptime(va_dict["dob"],
                                                  "%Y-%m-%d")
            dob = datetime.date(dob_temp.year,
                                dob_temp.month,
                                dob_temp.day)
        if isnull(va_dict["dod"]):
            event_date = datetime.date(9999, 9, 9)
        else:
            dod = datetime.datetime.strptime(va_dict["dod"],
                                             "%Y-%m-%d")
            event_date = datetime.date(dod.year,
                                       dod.month,
                                       dod.day)
        if va_dict["age"] >= 0 and not isnan(va_dict["age"]):
            age = int(va_dict["age"])
        else:
            age = "MISSING"
        if va_dict["cod"] == "Undetermined":
            cod_code = "99"
        else:
            cod_code = get_cod_code(self.dhis_cod_codes,
                                    va_dict["cod"])
        algorithm_metadata_code = va_dict['metadataCode']
        odk_id = va_dict['odkMetaInstanceID']

        e = VerbalAutopsyEvent(
            va_id,
            self.va_program_uid,
            org_unit,
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
            formatted_event = e.format_tea_to_dhis2(
                self.dhis_user,
                org_unit)
            return formatted_event
        else:
            formatted_event = e.format_se_to_dhis2(
                self.dhis_user,
                org_unit)
            return formatted_event

    def verify_post(self, post_log):
        """Verify that VA records were posted to DHIS2 server.

        :param post_log: Log information retrieved after posting events to
          a VA Program on a DHIS2 server; this is the return object from
          :meth:`DHIS.post_va <post_va>`.
        :type post_log: dictionary
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
                post = self.api_dhis.get("events/{}".format(va_reference))
                posted_data_values = post["dataValues"]
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
                df_new_storage.loc[row_va_id,
                                   "event_id"] = va_reference
                df_new_storage.loc[row_va_id,
                                   "dhis_org_unit"] = post["orgUnit"]
            df_new_storage.to_csv(self.dir_openva + "/new_storage.csv",
                                  index=False)
        except (requests.RequestException, ) as exc:
            raise DHISError(
                "Problem verifying posted records with DHIS.post_va..." +
                str(exc)) from exc

    def verify_tei_post(self, post_log):
        """Verify that VA tracked entity instances (tei) were posted to
        DHIS2 server.

        :param post_log: Log information retrieved after posting events to
          a VA Program on a DHIS2 server; this is the return object from
          :meth:`DHIS.post_va <post_va>`.
        :type post_log: dictionary
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
                posted_events = self.api_dhis.get("events",
                                                  params=params)["events"]
                if posted_events:
                    posted_event = posted_events[0]
                    posted_data_values = posted_event["dataValues"]
                    posted_va_id = [d["value"] for d in posted_data_values
                                    if d["dataElement"] == "htm6PixLJNy"]
                    new_col_names = ["pipelineOutcome",
                                     "tei_id",
                                     "event_id",
                                     "dhis_org_unit"]
                    new_col_values = ["Pushed to DHIS2",  # pipelineOutcome
                                      va_reference,  # tei_id
                                      posted_event["event"],  # event_id
                                      posted_event["orgUnit"]]  # dhis_org_unit
                    row_va_id = df_new_storage[
                                    "dhisVerbalAutopsyID"] == posted_va_id[0]
                    # df_new_storage.loc[row_va_id,
                    #                    "pipelineOutcome"] = "Pushed to DHIS2"
                    df_new_storage.loc[row_va_id,
                                       new_col_names] = new_col_values
            df_new_storage.to_csv(self.dir_openva + "/new_storage.csv",
                                  index=False)
        except (requests.RequestException, ) as exc:
            raise DHISError(
                "Problem verifying posted records with DHIS.post_va..." +
                str(exc)) from exc

    def verify_single_va(self,
                         va_id: str,
                         post_log: Dict) -> bool:
        """Verify that VA tracked entity instance (tei) was posted to
        DHIS2 server.

        :param va_id: Verbal Autopsy ID
        :type va_id: str
        :param post_log: Log information retrieved after posting events to
          a VA Program on a DHIS2 server; this is the return object from
          :meth:`DHIS.post_va <post_va>`.
        :type post_log: dictionary
        :raises: DHISError
        """

        if self.post_to_tracker:
            parse_log = self._parse_tei_post_log(post_log)
            log_summary = list(parse_log.values())[0]
            event_id = log_summary["event_id"]
        else:
            event_id = post_log["response"]["importSummaries"][0]["reference"]
        try:
            posted_event = self.api_dhis.get(f"events/{event_id}")
            posted_data_values = posted_event["dataValues"]
            posted_va_id = [d["value"] for d in posted_data_values
                            if d["dataElement"] == "htm6PixLJNy"]
            return posted_va_id[0] == va_id
        except (requests.RequestException, ) as exc:
            raise DHISError(
                "Problem verifying posted record with "
                "DHIS.post_single_va... " +
                str(exc)) from exc

    def _parse_tei_post_log(self, log: dict) -> dict:
        """Return events status for each tracked entity instance (TEI) as
        indicated by the object (log) returned from a POST to DHIS2.

        :param log: object returned from POST to DHIS2
        :returns: Event status and description for each TEI
        :rtype: dict
        """
        try:
            n_imported = log["response"]["imported"]
        except KeyError as exc:
            raise DHISError(
                "Problem with log summary returned by DHIS2 after trying to "
                "post TEIs (could not find log['response']['imported'])... " +
                str(exc)) from exc
        if n_imported == 0:
            return {}
        else:
            log_summaries = log["response"]["importSummaries"]
            tei_event_status = {}
            for summary in log_summaries:
                tei_ref = summary.get("reference")
                event_params = {"trackedEntityInstance": tei_ref}
                event_data = self.api_dhis.get("events", params=event_params)
                event_id = event_data["events"][0]["event"]
                event_status = summary.get("status")
                tei_event_status[tei_ref] = {"event_status": event_status,
                                             "tei_id": tei_ref,
                                             "event_id": event_id}
            return tei_event_status

    def _parse_post_log(self, log: dict) -> dict:
        """Return events status for each VA event as indicated by the object
        (log) returned from a POST to DHIS2.

        :param log: object returned from POST to DHIS2
        :returns: ID and status for each VA event
        :rtype: dict
        """
        try:
            n_imported = log["response"]["imported"]
        except KeyError as exc:
            raise DHISError(
                "Problem with log summary returned by DHIS2 after trying to "
                "post events (could not find log['response']['imported'])... " +
                str(exc)) from exc
        if n_imported == 0:
            return {}
        else:
            log_summaries = log["response"]["importSummaries"]
            event_status = {}
            for summary in log_summaries:
                event_id = summary.get("reference")
                status = summary.get("status")
                event_status[event_id] = {"event_id": event_id,
                                          "event_status": status}
            return event_status

    def _assign_va_program_to_org_units(self,
                                        org_unit: str) -> bool:
        """Assign Verbal Autopsy (VA) program to organisation units
        :param org_unit: UID for organisation unit that needs to be assigned
        the VA program
        :returns: Boolean for successful assignment
        :rtype: bool
        :raises: DHISError
        """
        existing = self.api_dhis.get(f"programs/{self.va_program_uid}",
                                     params={"field": "owner"})
        if org_unit not in [ou["id"] for ou in existing["organisationUnits"]]:
            existing["organisationUnits"].append({"id": org_unit})
            package = {"programs": [existing]}
            post_log = self.api_dhis.post("metadata", data=package)
            if post_log.get("status") == "SUCCESS":
                return True
            else:
                return False
        else:
            return True
