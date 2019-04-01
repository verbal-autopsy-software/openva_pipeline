"""
openva_pipeline.dhis
--------------------

This module posts VA records with assigned causes of death to a DHIS server.
"""

from pysqlcipher3 import dbapi2 as sqlcipher
import requests
from pandas import read_csv
from pandas import DataFrame
from pandas import isnull
from math import isnan
import sqlite3
import pickle
import os
import csv
import datetime
import json
import sqlite3
import re

from .exceptions import PipelineError
from .exceptions import DHISError

class API(object):
    """This class provides methods for interacting with the DHIS2 API.

    This class is called by an instance of the :class:`DHIS <DHIS>` to retrieve
    information from and post verbal autopsy records (and results) to a
    DHIS2 server that has the Verbal Autopsy program installed.

    :param dhisURL: Web address for DHIS2 server (e.g., "play.dhis2.org/demo").
    :type dhisURL: string
    :param dhisUser: Username for DHIS2 account.
    :type dhisUser: string
    :param dhisPassword: Password for DHIS2 account.
    :type dhisPassword: string
    :raises: DHISError
    """

    def __init__(self, dhisURL, dhisUser, dhisPass):

        if "/api" in dhisURL:
            raise DHISError \
            ("Please do not specify /api/ in the server argument: \
              e.g. --server=play.dhis2.org/demo")
        if dhisURL.startswith("localhost") or dhisURL.startswith("127.0.0.1"):
            dhisURL = "http://{}".format(dhisURL)
        elif dhisURL.startswith("http://"):
            dhisURL = dhisURL
        elif not dhisURL.startswith("https://"):
            dhisURL = "https://{}".format(dhisURL)
        self.auth = (dhisUser, dhisPass)
        self.url = "{}/api".format(dhisURL) ## possible new parameter

    def get(self, endpoint, params = None):
        """GET method for DHIS2 API.

        :rtype: dict
        """

        url = "{}/{}.json".format(self.url, endpoint)
        if not params:
            params = {}
        params["paging"] = False
        try:
            r = requests.get(url = url, params = params, auth = self.auth)
            if r.status_code != 200:
                raise DHISError("HTTP Code: {}".format(r.status_code))
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
                raise DHISError\
                    ("Problem with API.post..." +
                     "HTTP Code: {}...".format(r.status_code) +
                     str(r.text)
                     )
            else:
                return r.json()
        except requests.RequestException:
            raise DHISError\
                ("Problem with API.post..." + str(requests.RequestException))

    def post_blob(self, f):
        """ Post file to DHIS2 and return created UID for that file

        :rtype: str
        """

        url = "{}/fileResources".format(self.url)
        with open(f, "rb") as fName:
            files = {"file": (f, fName, "application/x-sqlite3", {"Expires": "0"})}
            try:
                r = requests.post(url, files=files, auth=self.auth)
                if r.status_code not in (200, 202):
                    raise DHISError\
                        ("Problem with API.post_blob..." +
                         "HTTP Code: {}...".format(r.status_code) +
                         str(r.text)
                        )
                else:
                    response = r.json()
                    file_id = response["response"]["fileResource"]["id"]
                    return file_id
            except requests.RequestException:
                raise DHISError\
                    ("Problem with API.post_blob..." +
                     str(requests.RequestException)
                     )

class VerbalAutopsyEvent(object):
    """Create DHIS2 event + a BLOB file resource

    :param va_id: UID for verbal autopsy record
      (used as a DHIS2 data element)
    :type va_id: string
    :param program: UID of the DHIS2's Verbal Autopsy program
    :type program: string
    :param dhis_orgunit: UID for the DHIS2 Organization Unit where the
      event (death) should be registered.
    :type dhis_orgunit: string
    :param event_date: Date of death with YYYY-MM-DD format
    :type event_date: datetime.date
    :param sex: Sex of the deceased (used as a DHIS2 data element).
      Possible values must fit to an option in the VA Program's "Sex"
      optionSet: female, male, missing, unknown).  If SmartVA is used
      to assign cause of death, then sex is an integer with 1 = male
      and 2 = female).
    :type sex: string or integer
    :param dob: Date of birth of the deceased with YYYY-MM-DD format
      (used as a DHIS2 data element)
    :type dob: datetime.date
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

    def __init__(self, cod_code, algorithm_metadata, file_id,
                 odk_meta_instanceID, va_id, serial_no, birth_certificate,
                 patient_nhn, surname, firstname, surname_va, firstname_va,
                 mothers_name, dob, event_date, age, sex, nationality,
                 religion, ethnicity, marital_status, place_of_death,
                 place_of_burial, occupation, current_province,
                 current_village, home_province, home_village,
                 program, dhis_orgunit):


        self.cod_code = cod_code
        self.algorithm_metadata = algorithm_metadata
        self.file_id = file_id

        self.odk_meta_instanceID = odk_meta_instanceID
        self.va_id = va_id
        self.serial_no = serial_no
        self.birth_certificate = birth_certificate
        self.patient_nhn = patient_nhn
        self.surname = surname
        self.firstname = firstname
        self.surname_va = surname_va
        self.firstname_va = firstname_va
        self.mothers_name = mothers_name
        #self.dob = datetime.datetime.strftime(dob, "%Y-%m-%d")
        self.dob = dob
        self.event_date = event_date
        self.age = age
        self.sex = sex
        self.nationality = nationality
        self.religion = religion
        self.ethnicity = ethnicity
        self.marital_status = marital_status
        self.place_of_death = place_of_death
        self.place_of_burial = place_of_burial
        self.occupation = occupation
        self.current_province = current_province
        self.current_village = current_village
        self.home_province = home_province
        self.home_village = home_village

        self.program = program
        self.dhis_orgunit = dhis_orgunit

        self.dataValues = [
                {"dataElement":"F4XGdOBvWww", "value": self.cod_code},
                {"dataElement":"wiJviUqN1io", "value": self.algorithm_metadata},
                {"dataElement":"XLHIBoLtjGt", "value": file_id}
            ]

        self.attributes = [
                {"attribute":"S1B2VQCRGdP", "value":self.odk_meta_instanceID},
                {"attribute":"j6GVXegen6N", "value":self.va_id},
                {"attribute":"k1jH5QP4TeN", "value":self.serial_no},
                {"attribute":"qpwSldeEnq6", "value":self.birth_certificate},
                {"attribute":"IrGS9suwzVM", "value":self.patient_nhn},
                {"attribute":"JqNKVDmsTBD", "value":self.surname},
                {"attribute":"ZrUoLrUGgGB", "value":self.firstname},
                {"attribute":"JoFxID9nD7o", "value":self.surname_va},
                {"attribute":"QWJnOskest4", "value":self.firstname_va},
                {"attribute":"fYp83ZhtG7r", "value":self.mothers_name},
                {"attribute":"QESMqvj3xTK", "value":self.dob},
                {"attribute":"WsG2U1N48nF", "value":self.event_date},
                {"attribute":"GyadmnzSD1T", "value":self.age},
                {"attribute":"Ua3q51XEAoe", "value":self.sex},
                {"attribute":"cG5BSTK6EPb", "value":self.nationality},
                {"attribute":"ewd9j8nlRwl", "value":self.religion},
                {"attribute":"BY9KlYHP5CV", "value":self.ethnicity},
                {"attribute":"Vq5nON28XS8", "value":self.marital_status},
                {"attribute":"KaXF9HBQCdd", "value":self.place_of_death},
                {"attribute":"umTOigSn0Pz", "value":self.place_of_burial},
                {"attribute":"p54LYtetaIH", "value":self.occupation},
                {"attribute":"rAc7lR0gRQk", "value":self.current_province},
                {"attribute":"wrOtALb4UXO", "value":self.current_village},
                {"attribute":"HTMOSXzn3QT", "value":self.home_province},
                {"attribute":"sS91Pi3fJ3M", "value":self.home_village}
        ]
        self.events = [
          {
            "program":self.program,
            "orgUnit":self.dhis_orgunit,
            "eventDate":self.event_date,
            "status":"COMPLETED",
            "programStage":"JlEmwONw9Is",
            "dataValues": self.dataValues
          }
        ]
        self.enrollments = [
          {
            "orgUnit":self.dhis_orgunit,
            "program":self.program,
            "events":self.events
          }
        ]

    def format_to_dhis2(self, dhisUser):
        """
        Format object to DHIS2 compatible event for DHIS2 API

        :param dhisUser: DHIS2 username for account posting the event
        :returns: DHIS2 event
        :rtype: dict
        """
        trackedEntityInstance = {
            "trackedEntityType": "MCPQUTHX1Ze",
            "orgUnit": self.dhis_orgunit,
            "attributes": self.attributes,
            "enrollments": self.enrollments
        }
        return trackedEntityInstance

    def format_existing_tei_to_dhis2(self, dhisUser, tei_id):
        """
        Format object to DHIS2 compatible event for DHIS2 API for a
        tracked entity instance (TEI) that is already registered.

        :param dhisUser: DHIS2 username for account posting the event
        :returns: DHIS2 event
        :rtype: dict
        """
        trackedEntityInstance = {
            "tracedkEntityInstance": tei_id,
            "trackedEntityType": "MCPQUTHX1Ze",
            "orgUnit": self.dhis_orgunit,
            "attributes": self.attributes,
            "enrollments": self.enrollments
        }
        return trackedEntityInstance

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__)

def create_db(fName, evaList):
    """
    Create a SQLite database with VA data + COD

    :param evaList: Event-Value-Attribute data structure with verbal autopsy
      data, cause of death result, and VA metadata.
    :type evaList: list
    :rtype: None
    """
    conn = sqlite3.connect(fName)
    with conn:
        cur = conn.cursor()
        cur.execute("CREATE TABLE vaRecord(ID INT, Attrtibute TEXT, Value TEXT)")
        cur.executemany("INSERT INTO vaRecord VALUES (?,?,?)", evaList)
    conn.close()


def getCODCode(myDict, searchFor):
    """Return COD label expected by (DHIS2) VA Program.

    :param searchFor: Cause of Death label returned by openVA.
    :type searchFor: string
    :rtype: str
    """
    for i in range(len(myDict.keys())):
        match = re.search(searchFor, list(myDict.keys())[i])
        if match:
            return list(myDict.values())[i]

def findKeyValue(key, d):
    """
    Return a key's value in a nested dictionary.
    """
    if key in d:
        yield d[key]
    for k in d:
        if isinstance(d[k], list):
            for i in d[k]:
                for j in findKeyValue(key, i):
                    yield j

class DHIS():
    """Class for transfering VA records (with assigned CODs) to the DHIS2 server.

    This class includes methods for importing VA results (i.e. assigned causes of
    death from openVA or SmartVA) as CSV files, connecting to a DHIS2 server
    with the Verbal Autopsy Program, and posting the results to the DHIS2
    server and/or the local Transfer database.

    :param dhisArgs: Contains parameter values for connected to DHIS2, as
      returned by transferDB.configDHIS().
    :type dhisArgs: (named) tuple
    :param workingDirectory: Workind direcotry for the openVA Pipeline
    :type workingDirectory: string
    :raises: DHISError
    """

    def __init__(self, dhisArgs, workingDirectory, orgUnitCodes):

        self.dhisURL = dhisArgs[0].dhisURL
        self.dhisUser = dhisArgs[0].dhisUser
        self.dhisPassword = dhisArgs[0].dhisPassword
        self.dhisCODCodes = dhisArgs[1]
        self.dirDHIS = os.path.join(workingDirectory, "DHIS")
        self.dirOpenVA = os.path.join(workingDirectory, "OpenVAFiles")
        self.vaProgramUID = None
        self.nPostedRecords = 0
        self.orgUnitCodes = orgUnitCodes

        dhisPath = os.path.join(workingDirectory, "DHIS")

        try:
            if not os.path.isdir(dhisPath):
                os.makedirs(dhisPath)
        except:
            raise DHISError("Unable to create directory" + dhisPath)

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
            apiDHIS = API(self.dhisURL, self.dhisUser, self.dhisPassword)
        except (requests.RequestException) as e:
            raise DHISError(str(e))

        vaPrograms = apiDHIS.get("programs",
                                 params={"filter": "name:like:Verbal Autopsy"}
                                 ).get("programs")
        if len(vaPrograms) == 0:
            raise DHISError("No Verbal Autopsy Program found.")
        if len(vaPrograms) > 1:
            raise DHISError("More than one Verbal Autopsy Program found.")
        self.vaProgramUID = vaPrograms[0].get("id")

        return apiDHIS

    def postVA(self, apiDHIS):
        """Post VA records to DHIS.

        This method reads in a CSV file ("entityAttribuesValue.csv") with
        cause of death results (from openVA) then formats events and posts
        them to a VA Program (installed on DHIS2 server).

        :param apiDHIS: A class instance for interacting with the DHIS2 API
          created by the method :meth:`DHIS.connect <connect>`
        :type apiDHIS: Instance of the :class:`API <API>` class
        :returns: Log information receieved after posting events to the VA
          Program on a DHIS2 server (see :meth:`API.post <API.post>`).
        :rtype: dict
        :raises: DHISError
        """

        evaPath = os.path.join(self.dirOpenVA, "entityAttributeValue.csv")
        if not os.path.isfile(evaPath):
            raise DHISError("Missing: " + evaPath)
        recordStoragePath = os.path.join(self.dirOpenVA, "recordStorage.csv")
        if not os.path.isfile(evaPath):
            raise DHISError("Missing: " + recordStoragePath)
        newStoragePath = os.path.join(self.dirOpenVA, "newStorage.csv")

        blobPath = os.path.join(self.dirDHIS, "blobs")
        try:
            if not os.path.isdir(blobPath):
                os.makedirs(blobPath)
        except OSError as e:
            raise DHISError("Unable to create directory for DHIS blobs.")

        trackedEntityInstances = []
        export = {}

        dfDHIS = read_csv(evaPath)
        grouped = dfDHIS.groupby(["ID"])
        dfRecordStorage = read_csv(recordStoragePath)

        with open(newStoragePath, "w", newline="") as csvOut:
            # writer = csv.writer(csvOut, lineterminator="\n")
            writer = csv.writer(csvOut)

            header = list(dfRecordStorage)
            header = [x.lower() for x in header]
            header.extend(["dhisVerbalAutopsyID", "pipelineOutcome"])
            writer.writerow(header)
            dhis_orgunit_index = header.index("general5-location-facility_death")
            serial_no_index = header.index("generalmodule-sid")
            surname_va_index = header.index("general5-name-gen_5_0a")
            firstname_va_index = header.index("general5-name-gen_5_0")

            ## this depends on openVA vs SmartVA
            for i in dfRecordStorage.itertuples(index = False):
                row = list(i)
                if row[5] != "MISSING" and row[5] != None and type(row[5]) != float:

                    if row[5] == "Undetermined":
                        cod_code = "99"
                    else:
                        cod_code = getCODCode(self.dhisCODCodes, row[5])

                    algorithm_metadata = row[6]
                    odk_meta_instanceID = str(row[0])
                    va_id = row[serial_no_index]
                    serial_no = row[serial_no_index]

                    blobFile = "{}.db".format(
                        #os.path.join(self.dirDHIS, "blobs", va_id)
                        os.path.join(self.dirDHIS, "blobs", odk_meta_instanceID)
                    )
                    blobRecord = grouped.get_group(str(row[0]))
                    blobEVA = blobRecord.values.tolist()
                    try:
                        create_db(blobFile, blobEVA)
                    except:
                        raise DHISError("Unable to create blob.")
                    try:
                        file_id = apiDHIS.post_blob(blobFile)
                    except requests.RequestException as e:
                        raise DHISError\
                            ("Unable to post blob to DHIS..." + str(e))

                    surname_va = row[surname_va_index]
                    firstname_va = row[firstname_va_index]

                    if isnull(row[2]):
                        dob = str(datetime.date(9999,9,9))
                    else:
                        dobTemp = datetime.datetime.strptime(row[2], "%Y-%m-%d")
                        dob = str(datetime.date(dobTemp.year, dobTemp.month, dobTemp.day))

                    if isnull(row[3]):
                        event_date = str(datetime.date(9999,9,9))
                    else:
                        dod = datetime.datetime.strptime(row[3], "%Y-%m-%d")
                        event_date = str(datetime.date(dod.year, dod.month, dod.day))

                    if type(row[4]) == float and not isnan(row[4]):
                        age = int(row[4])
                    else:
                        age = "MISSING"

                    algorithm = row[6].split("|")[0]
                    if algorithm == "Tariff":
                        if row[1] in ["1", "1.0", 1, 1.0]:
                            sex = "MALE"
                        elif row[1] in ["2", "2.0", 2, 2.0]:
                            sex = "FEMALE"
                        else:
                            sex = "NOT_STATED"
                    else:
                        sex = row[1].lower()

                    if (type(row[dhis_orgunit_index]) == float and isnan(row[dhis_orgunit_index])):
                        dhis_orgunit = 'NtlgKoJBimp'
                    if (type(row[dhis_orgunit_index]) == str and len(row[dhis_orgunit_index]) == 0):
                        dhis_orgunit = 'NtlgKoJBimp'
                    if (type(row[dhis_orgunit_index]) == str and len(row[dhis_orgunit_index]) > 0):
                        odkOrgUnitCode = row[dhis_orgunit_index][0:6]
                        odkOrgUnitRow = self.orgUnitCodes.loc[self.orgUnitCodes['odkCode'] == odkOrgUnitCode]
                        if odkOrgUnitRow.shape[0] != 1:
                            dhis_orgunit = 'NtlgKoJBimp'
                        else:
                            dhis_orgunit = self.orgUnitCodes.iat[odkOrgUnitRow.index[0], 2]
                        
                    dhis_filter = 'k1jH5QP4TeN:EQ:{}'.format(serial_no)
                    params = {'filter': dhis_filter, 'ouMode': 'ALL', 'fields': 'attributes'}
                    dhis_attributes_raw = apiDHIS.get(endpoint='trackedEntityInstances',
                                                      params=params)
                    if len(dhis_attributes_raw['trackedEntityInstances']) == 0:
                        dhis_attributes = {}
                    else:
                        dhis_attributes = dhis_attributes_raw['trackedEntityInstances'][0]['attributes']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Birth Certificate No']
                    if len(tmp_attribute) == 0:
                        birth_certificate = ''
                    else:
                        birth_certificate = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Patient NHN']
                    if len(tmp_attribute) == 0:
                        patient_nhn = ''
                    else:
                        patient_nhn = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Surname']
                    if len(tmp_attribute) == 0:
                        surname = ''
                    else:
                        surname = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'First and Middle Names']
                    if len(tmp_attribute) == 0:
                        firstname = ''
                    else:
                        firstname = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == "Mother's Name"]
                    if len(tmp_attribute) == 0:
                        mothers_name = ''
                    else:
                        mothers_name = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Nationality']
                    if len(tmp_attribute) == 0:
                        nationality = ''
                    else:
                        nationality = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Religion']
                    if len(tmp_attribute) == 0:
                        religion = ''
                    else:
                        religion = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Ethnicity']
                    if len(tmp_attribute) == 0:
                        ethnicity = ''
                    else:
                        ethnicity = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Marital Status']
                    if len(tmp_attribute) == 0:
                        marital_status = ''
                    else:
                        marital_status = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Place of Death']
                    if len(tmp_attribute) == 0:
                        place_of_death = ''
                    else:
                        place_of_death = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Place of Burial']
                    if len(tmp_attribute) == 0:
                        place_of_burial = ''
                    else:
                        place_of_burial = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Occupation']
                    if len(tmp_attribute) == 0:
                        occupation = ''
                    else:
                        occupation = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Current province']
                    if len(tmp_attribute) == 0:
                        current_province = ''
                    else:
                        current_province = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Current village']
                    if len(tmp_attribute) == 0:
                        current_village = ''
                    else:
                        current_village = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Home province']
                    if len(tmp_attribute) == 0:
                        home_province = ''
                    else:
                        home_province = tmp_attribute['value']
                    tmp_attribute = [d for d in dhis_attributes if d['displayName'] == 'Home village']
                    if len(tmp_attribute) == 0:
                        home_village = ''
                    else:
                        home_village = tmp_attribute['value']
                    
                    e = VerbalAutopsyEvent(cod_code, algorithm_metadata,
                                           file_id, odk_meta_instanceID, va_id,
                                           serial_no, birth_certificate,
                                           patient_nhn, surname, firstname,
                                           surname_va, firstname_va,
                                           mothers_name, dob, event_date, age,
                                           sex, nationality, religion,
                                           ethnicity, marital_status,
                                           place_of_death, place_of_burial,
                                           occupation, current_province,
                                           current_village, home_province,
                                           home_village, self.vaProgramUID,
                                           dhis_orgunit)
                    dhis_filter = 'k1jH5QP4TeN:EQ:{}'.format(serial_no)
                    params = {'filter': dhis_filter, 'ouMode': 'ALL', 'fields': '*'}
                    tei_filter = apiDHIS.get(endpoint='trackedEntityInstances', params=params)
                    tei_list = tei_filter['trackedEntityInstances']
                    if len(tei_list) == 0:
                        trackedEntityInstances.append(e.format_to_dhis2(self.dhisUser))
                    else:
                        tei_index = [index for (index, j) in enumerate(tei_list)
                                       for i in j["attributes"]
                                         if i["attribute"] == "S1B2VQCRGdP" and
                                            i["value"] == odk_meta_instanceID]
                        if len(tei_index) == 0:
                            trackedEntityInstances.append(e.format_to_dhis2(self.dhisUser))
                        else:
                            tei_id = tei_list[tei_index[0]]["trackedEntityInstance"]
                            trackedEntityInstances.append(
                                e.format_existing_tei_to_dhis2(self.dhisUser, tei_id)
                                )

                    #row.extend([va_id, "Pushing to DHIS2"])
                    row.extend([odk_meta_instanceID, "Pushing to DHIS2"])
                    writer.writerow(row)
                else:
                    row.extend(["", "No CoD Assigned"])
                    writer.writerow(row)
        export["trackedEntityInstances"] = trackedEntityInstances
        try:
            log = apiDHIS.post("trackedEntityInstances", data=export)
        except requests.RequestException as e:
            raise DHISError("Unable to post events to DHIS2..." + str(e))
        self.nPostedRecords = len(log['response']['importSummaries'])
        return log

    def verifyPost(self, postLog, apiDHIS):
        """Verify that VA records were posted to DHIS2 server.

        :param postLog: Log information retrieved after posting events to
          a VA Program on a DHIS2 server; this is the return object from
          :meth:`DHIS.postVA <postVA>`.
        :type postLog: dictionary
        :param apiDHIS: A class instance for interacting with the DHIS2 API
          created by the method :meth:`DHIS.connect <connect>`
        :type apiDHIS: Instance of the :class:`API <API>` class
        :raises: DHISError
        """

        vaReferences = list(findKeyValue("reference", d = postLog["response"]))
        try:
            dfNewStorage = read_csv(self.dirOpenVA + "/newStorage.csv")
        except:
            raise DHISError\
                ("Problem with DHIS.verifyPost...Can't find file " +
                 self.dirOpenVA + "/newStorage.csv")
        try:
            for vaReference in vaReferences:
                postedDataValues = apiDHIS.get("trackedEntityInstances/{}".format(vaReference)).get("attributes")
                postedVAIDIndex  = next((index for (index, d) in enumerate(postedDataValues) if d["attribute"]=="S1B2VQCRGdP"), None)
                postedVAID       = postedDataValues[postedVAIDIndex]["value"]
                rowVAID          = dfNewStorage["id"] == postedVAID
                dfNewStorage.loc[rowVAID,"pipelineOutcome"] = "Pushed to DHIS2"
            dfNewStorage.to_csv(self.dirOpenVA + "/newStorage.csv", index = False)
        except:
            raise DHISError\
                ("Problem with DHIS.postVA...couldn't verify posted records.")
