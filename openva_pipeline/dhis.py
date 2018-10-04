#------------------------------------------------------------------------------#
# dhis.py
#-----------------------------------------------------------------------------#
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
from pipeline import PipelineError

class API(object):
    """This class provides methods for interacting with the DHIS2 API.

    Parameters
    ----------
    dhisURL : str
        Web address for DHIS2 server (e.g., "play.dhis2.org/demo").
    dhisUser : str
        Username for DHIS2 account.
    dhisPassword : str
        Password for DHIS2 account.

    Methods
    -------
    get(self, endpoint, params)
        GET method for DHIS2 API.
    post(self, endpoint, data) 
        POST method for DIHS2 API.
    post_blob(self, f) 
        Post file to DHIS2 and return created UID for that file.

    Raises
    ------
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
                print("HTTP Code: {}".format(r.status_code)) ## HERE
                print(r.text)
            else:
                return r.json()
        except requests.RequestException:
            raise requests.RequestException

    def post_blob(self, f):
        """ Post file to DHIS2 and return created UID for that file
        :rtype: str
        """
        url = "{}/fileResources".format(self.url)
        files = {"file": (f, open(f, "rb"), "application/x-sqlite3", {"Expires": "0"})}
        try:
            r = requests.post(url, files=files, auth=self.auth)
            if r.status_code not in (200, 202):
                print("HTTP Code: {}".format(r.status_code)) ## HERE
                print(r.text)
            else:
                response = r.json()
                file_id = response["response"]["fileResource"]["id"]
                return file_id
        except requests.RequestException:
            raise requests.RequestException

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
    Return COD label expected by DHIS.
    :rtype: str
    """
    for i in range(len(myDict.keys())):
        match = re.search(searchFor, list(myDict.keys())[i])
        if match:
            return list(myDict.values())[i]


class DHIS():
    """Class for transfering VA records (with assigned CODs) to the DHIS server.

    This class includes methods for importing VA results (i.e. assigned causes of
    death from openVA or SmartVA) as CSV files, connecting to a DHIS2 server
    with the Verbal Autopsy Program, and posting the results to the DHIS2
    server and/or the local Transfer database.

    Parameters
    ----------
    dhisArgs : (named) tuple
        Contains parameter values for connected to DHIS, as returned by
        transferDB.configDHIS().

    Methods
    -------
    connect(self)
        Wrapper for algorithm-specific methods that create an R script to use
        openVA to assign CODs.
    postVA(self)
        Prepare and post VA objects to the DHIS2 server.
    storeDB(self)
        Deposits VA objects to the Transfer DB.
    CheckDuplicates(self)
        Checks the DHIS2 server for duplicate records.

    Raises
    ------
    DHISError

    """

    def __init__(self, dhisArgs, workingDirectory):
        
        self.dhisURL = dhisArgs[0].dhisURL
        self.dhisUser = dhisArgs[0].dhisUser
        self.dhisPassword = dhisArgs[0].dhisPassword
        self.dhisOrgUnit = dhisArgs[0].dhisOrgUnit
        # self.dhisCODCodes = dhisArgs.dhisCODCodes
        self.dhisCODCodes = dhisArgs[1]
        self.dirDHIS = os.path.join(workingDirectory, "DHIS2")
        self.dirOpenVA = os.path.join(workingDirectory, "OpenVAFiles")
        self.vaProgramUID = None

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
        return(apiDHIS)

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

        dfDHIS2 = read_csv(evaPath)
        grouped = dfDHIS2.groupby(["ID"])
        with open(recordStoragePath, "r", newline="") as csvIn:
            with open(newStoragePath, "w", newline="") as csvOut:
                reader = csv.reader(csvIn)
                writer = csv.writer(csvOut, lineterminator="\n")

                header = next(reader)
                header.extend(["dhisVerbalAutopsyID", "pipelineOutcome"])
                writer.writerow(header)

                for row in reader:
                    if row[5]!="MISSING":

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
            raise DHISError("Unable to post events to DHIS..." + str(e))
        return(log)

#------------------------------------------------------------------------------#
# Exceptions
#------------------------------------------------------------------------------#
class DHISError(PipelineError): pass
