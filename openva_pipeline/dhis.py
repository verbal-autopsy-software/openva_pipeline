"""
openva_pipeline.dhis
--------------------

This module posts VA records with assigned causes of death to a DHIS server.
"""

from pysqlcipher3 import dbapi2 as sqlcipher
import requests
from pandas import read_csv, groupby
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

    Parameters

    dhisURL : str
        Web address for DHIS2 server (e.g., "play.dhis2.org/demo").
    dhisUser : str
        Username for DHIS2 account.
    dhisPassword : str
        Password for DHIS2 account.

    Methods

    get(self, endpoint, params)
        GET method for DHIS2 API.
    post(self, endpoint, data) 
        POST method for DIHS2 API.
    post_blob(self, f) 
        Post file to DHIS2 and return created UID for that file.

    Raises

    DHISError

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
        """
        GET method for DHIS2 API.
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
        """
        POST  method for DHIS2 API.
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
    """ DHIS2 event + a BLOB file resource"""

    def __init__(self, va_id, program, dhis_orgunit, event_date, sex, dob, age,
                 cod_code, algorithm_metadata, odk_id, file_id):
        self.va_id = va_id
        self.program = program
        self.dhis_orgunit = dhis_orgunit
        self.event_date = event_date
        self.sex = sex
        self.dob = dob
        self.age = age
        self.cod_code = cod_code
        self.algorithm_metadata = algorithm_metadata
        self.odk_id = odk_id

        self.datavalues = [
            {"dataElement": "htm6PixLJNy", "value": self.va_id},
            {"dataElement": "hi7qRC4SMMk", "value": self.sex},
            {"dataElement": "mwSaVq64k7j", "value": self.dob},
            {"dataElement": "F4XGdOBvWww", "value": self.cod_code},
            {"dataElement": "wiJviUqN1io", "value": self.algorithm_metadata},
            {"dataElement": "oPAg4MA0880", "value": self.age},
            {"dataElement": "LwXZ2dZmJb0", "value": self.odk_id},
            {"dataElement": "XLHIBoLtjGt", "value": file_id}

        ]

    def format_to_dhis2(self, dhisUser):
        """
        Format object to DHIS2 compatible event for DHIS2 API
        :rtype: dict
        """
        event = {
            "program": self.program,
            "orgUnit": self.dhis_orgunit,
            "eventDate": datetime.datetime.strftime(self.event_date, "%Y-%m-%d"),
            "status": "COMPLETED",
            "storedBy": dhisUser,
            "dataValues": self.datavalues
        }
        return event

    def __str__(self):
        return json.dumps(self, default=lambda o: o.__dict__)

def create_db(fName, evaList):
    """
    Create a SQLite database with VA data + COD
    :rtype: None
    """
    conn = sqlite3.connect(fName)
    with conn:
        cur = conn.cursor()
        cur.execute("CREATE TABLE vaRecord(ID INT, Attrtibute TEXT, Value TEXT)")
        cur.executemany("INSERT INTO vaRecord VALUES (?,?,?)", evaList)
    conn.close()


def getCODCode(myDict, searchFor):
    """
    Return COD label expected by DHIS2.
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
    """Class for transfering VA records (with assigned CODs) to the DHIf2S server.

    This class includes methods for importing VA results (i.e. assigned causes of
    death from openVA or SmartVA) as CSV files, connecting to a DHIS2 server
    with the Verbal Autopsy Program, and posting the results to the DHIS2
    server and/or the local Transfer database.

    Parameters

    dhisArgs : (named) tuple
        Contains parameter values for connected to DHIS2, as returned by
        transferDB.configDHIS().

    Methods

    connect(self)
        Wrapper for algorithm-specific methods that create an R script to use
        openVA to assign CODs.
    postVA(self)
        Prepare and post VA objects to the DHIS2 server.
    verifyPost(self)
        Verify that VA records were posted to DHIS2 server.
    CheckDuplicates(self)
        Checks the DHIS2 server for duplicate records.

    Raises

    DHISError

    """

    def __init__(self, dhisArgs, workingDirectory):
        
        self.dhisURL = dhisArgs[0].dhisURL
        self.dhisUser = dhisArgs[0].dhisUser
        self.dhisPassword = dhisArgs[0].dhisPassword
        self.dhisOrgUnit = dhisArgs[0].dhisOrgUnit
        # self.dhisCODCodes = dhisArgs.dhisCODCodes
        self.dhisCODCodes = dhisArgs[1]
        self.dirDHIS = os.path.join(workingDirectory, "DHIS")
        self.dirOpenVA = os.path.join(workingDirectory, "OpenVAFiles")
        self.vaProgramUID = None
        self.nPostedRecords = 0

        dhisPath = os.path.join(workingDirectory, "DHIS")

        try:
            if not os.path.isdir(dhisPath):
                os.makedirs(dhisPath)
        except:
            raise DHISError("Unable to create directory" + dhisPath)

    def connect(self):
        """Run get method to retrieve VA program ID and Org Unit."""

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

        orgUnits = apiDHIS.get("organisationUnits").get("organisationUnits")
        validOrgUnit = self.dhisOrgUnit in [i["id"] for i in orgUnits]
        if not validOrgUnit:
            raise DHISError("Did not find Organisation Unit.")
        return apiDHIS

    def postVA(self, apiDHIS):
        """Post VA records to DHIS."""
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

        events = []
        export = {}

        dfDHIS = read_csv(evaPath)
        grouped = dfDHIS.groupby(["ID"])
        dfRecordStorage = read_csv(recordStoragePath)

        with open(newStoragePath, "w", newline="") as csvOut:
            # writer = csv.writer(csvOut, lineterminator="\n")
            writer = csv.writer(csvOut)

            header = list(dfRecordStorage)
            header.extend(["dhisVerbalAutopsyID", "pipelineOutcome"])
            writer.writerow(header)

            ## this depends on openVA vs SmartVA
            for i in dfRecordStorage.itertuples(index = False):
                row = list(i)

                if row[5] != "MISSING" and row[5] != None:

                    vaID = str(row[0])
                    blobFile = "{}.db".format(
                        os.path.join(self.dirDHIS, "blobs", vaID)
                    )
                    blobRecord = grouped.get_group(str(row[0]))
                    blobEVA = blobRecord.values.tolist()

                    try:
                        create_db(blobFile, blobEVA)
                    except:
                        raise DHISError("Unable to create blob.")
                    try:
                        fileID = apiDHIS.post_blob(blobFile)
                    except requests.RequestException as e:
                        raise DHISError\
                            ("Unable to post blob to DHIS..." + str(e))

                    algorithm = row[6].split("|")[0]
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
                    dob = row[2]
                    if row[3] =="":
                        eventDate = datetime.date(9999,9,9)
                    else:
                        dod = datetime.datetime.strptime(row[3], "%Y-%m-%d")
                        eventDate = datetime.date(dod.year, dod.month, dod.day)
                    age = row[4]
                    if row[5] == "Undetermined":
                        codCode = "99"
                    else:
                        codCode = getCODCode(self.dhisCODCodes, row[5])
                    algorithmMetadataCode = row[6]
                    odkID = row[7]

                    e = VerbalAutopsyEvent(vaID, self.vaProgramUID, self.dhisOrgUnit,
                                           eventDate, sex, dob, age,
                                           codCode, algorithmMetadataCode,
                                           odkID, fileID)
                    events.append(e.format_to_dhis2(self.dhisUser))

                    row.extend([vaID, "Pushing to DHIS2"])
                    writer.writerow(row)
                else:
                    row.extend(["", "No CoD Assigned"])
                    writer.writerow(row)
        export["events"] = events
        try:
            log = apiDHIS.post("events", data=export)
        except requests.RequestException as e:
            raise DHISError("Unable to post events to DHIS2..." + str(e))
        self.nPostedRecords = len(log['response']['importSummaries'])
        return log

    def verifyPost(self, postLog, apiDHIS):
        """Verify that VA records were posted to DHIS2 server."""

        vaReferences = list(findKeyValue("reference", d = postLog["response"]))
        try:
            dfNewStorage = read_csv(self.dirOpenVA + "/newStorage.csv")
        except:
            raise DHISError\
                ("Problem with DHIS.verifyPost...Can't find file " +
                 self.dirOpenVA + "/newStorage.csv")
        try:
            for vaReference in vaReferences:
                postedDataValues = apiDHIS.get("events/{}".format(vaReference)).get("dataValues")
                postedVAIDIndex  = next((index for (index, d) in enumerate(postedDataValues) if d["dataElement"]=="htm6PixLJNy"), None)
                postedVAID       = postedDataValues[postedVAIDIndex]["value"]
                rowVAID          = dfNewStorage["dhisVerbalAutopsyID"] == postedVAID
                dfNewStorage.loc[rowVAID,"pipelineOutcome"] = "Pushed to DHIS2"
            dfNewStorage.to_csv(self.dirOpenVA + "/newStorage.csv", index = False)
        except:
            raise DHISError\
                ("Problem with DHIS.postVA...couldn't verify posted records.")

