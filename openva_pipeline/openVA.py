"""
openva_pipeline.openVA
----------------------

This module runs openVA and smartVA to assign causes of death to VA records.
"""

import subprocess
import shutil
import os
from pandas import read_csv
from pandas import DataFrame
from pandas import concat
from pandas import merge
import numpy as np
from pycrossva.transform import transform

from .exceptions import OpenVAError
from .exceptions import SmartVAError


class OpenVA:
    """Assign cause of death (COD) to verbal autopsies (VA) R package openVA.

    This class creates and executes an R script that copies (and merges)
    ODK Briefcase exports, runs openVA to assign CODs, and creates outputs for
    depositing in the Transfers DB and to a DHIS server.

    :param algorithm: Which VA algorithm should be used to assign COD.
    :type algorithm: str
    :raises: OpenVAError
    """

    def __init__(self, vaArgs, pipelineArgs, odkID, runDate):

        self.vaArgs = vaArgs
        self.pipelineArgs = pipelineArgs
        if odkID is None:
            self.odkID = "meta-instanceID"
        else:
            self.odkID = odkID
        self.runDate = runDate

        dirOpenVA = os.path.join(pipelineArgs.workingDirectory, "OpenVAFiles")
        self.dirOpenVA = dirOpenVA
        dirODK = os.path.join(pipelineArgs.workingDirectory, "ODKFiles")
        self.dirODK = dirODK

        self.cliSmartVA = os.path.join(pipelineArgs.workingDirectory,
                                       "smartva")
        self.successfulRun = None

        try:
            if not os.path.isdir(dirOpenVA):
                os.makedirs(dirOpenVA)
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Unable to create directory" +
                              dirOpenVA) from exc

    def copyVA(self):
        """Create data file for openVA by merging ODK export files & converting
           with pycrossva.

        :returns: Indicator of an empty (i.e. no records) ODK export file
        :rtype: logical
        """

        exportFile_prev = os.path.join(self.dirODK, "odkBCExportPrev.csv")
        exportFile_new = os.path.join(self.dirODK, "odkBCExportNew.csv")
        pycvaInput = os.path.join(self.dirOpenVA, "pycrossva_input.csv")
        openVAInputFile = os.path.join(self.dirOpenVA, "openVA_input.csv")

        isExportFile_prev = os.path.isfile(exportFile_prev)
        isExportFile_new = os.path.isfile(exportFile_new)

        zeroRecords = False

        algorithmMetadata = self.pipelineArgs.algorithmMetadataCode.split("|")
        whoInstrumentVersion = algorithmMetadata[5]
        if whoInstrumentVersion == "v1_4_1":
            pcva_input = "2016WHOv141"
        else:
            pcva_input = "2016WHOv151"

        if isExportFile_new and not isExportFile_prev:
            with open(exportFile_new, "r", newline="") as fNew:
                fNewLines = fNew.readlines()
            if len(fNewLines) == 1:
                zeroRecords = True
                return zeroRecords
            shutil.copy(exportFile_new, pycvaInput)
            if self.pipelineArgs.algorithm == "SmartVA":
                shutil.copy(pycvaInput, openVAInputFile)
            else:
                final_data = transform(mapping=(pcva_input, "InterVA5"),
                                       raw_data=pycvaInput,
                                       raw_data_id=self.odkID,
                                       verbose=0)
                final_data.to_csv(openVAInputFile, index=False)

        if isExportFile_new and isExportFile_prev:
            with open(exportFile_new, "r", newline="") as fNew:
                fNewLines = fNew.readlines()
            with open(exportFile_prev, "r", newline="") as fPrev:
                fPrevLines = fPrev.readlines()

            if len(fNewLines) == 1 and len(fPrevLines) == 1:
                zeroRecords = True
                return zeroRecords

            shutil.copy(exportFile_new, pycvaInput)
            with open(pycvaInput, "a", newline="") as fCombined:
                for line in fPrevLines:
                    if line not in fNewLines:
                        fCombined.write(line)
            if self.pipelineArgs.algorithm == "SmartVA":
                shutil.copy(pycvaInput, openVAInputFile)
            else:
                final_data = transform(mapping=(pcva_input, "InterVA5"),
                                       raw_data=pycvaInput,
                                       raw_data_id=self.odkID,
                                       verbose=0)
                final_data.to_csv(openVAInputFile, index=False)
        return zeroRecords

    def rScript(self):
        """Create an R script for running openVA and assigning CODs."""

        if not self.pipelineArgs.algorithm == "SmartVA":
            try:
                os.makedirs(
                    os.path.join(self.dirOpenVA, self.runDate)
                )
            except (PermissionError, OSError) as exc:
                raise OpenVAError("Unable to create openVA dir" +
                                  str(exc)) from exc

        if self.pipelineArgs.algorithm == "InSilicoVA":
            self._rScript_insilicoVA()
        if self.pipelineArgs.algorithm == "InterVA":
            self._rScript_interVA()

    def _rScript_insilicoVA(self):

        fileName = os.path.join(self.dirOpenVA,
                                self.runDate,
                                "Rscript_" + self.runDate + ".R")
        algorithmMetadata = self.pipelineArgs.algorithmMetadataCode.split("|")
        whoInstrumentVersion = algorithmMetadata[5]
        raw_data_file = os.path.join(self.dirOpenVA, "pycrossva_input.csv")

        if whoInstrumentVersion not in ["v1_4_1", "v1_5_1", "v1_5_3"]:
            raise OpenVAError("pyCrossVA not able to process WHO " +
                              "instrument version: " + whoInstrumentVersion)
        try:
            with open(fileName, "w", newline="") as f:
                f.write("date() \n")
                f.write("library(openVA) \n")
                f.write("getwd() \n")
                f.write("raw_data <- read.csv('" + raw_data_file + "') \n")
                odk_id_for_r = self.odkID.replace("-", ".")
                f.write("raw_data_sorted <- raw_data[order(raw_data$" + odk_id_for_r + "),] \n")
                f.write("data_from_pycrossva <- read.csv('" + self.dirOpenVA + "/openVA_input.csv') \n")
                f.write("records_sorted <- data_from_pycrossva[order(data_from_pycrossva$ID),] \n")
                f.write("results <- insilico(data = records_sorted, \n")
                f.write("\t data.type = '" + self.vaArgs.InSilicoVA_data_type + "', \n")
                f.write("\t isNumeric = " + self.vaArgs.InSilicoVA_isNumeric + ", \n")
                f.write("\t updateCondProb = " + self.vaArgs.InSilicoVA_updateCondProb + ", \n")
                f.write("\t keepProbbase.level = " + self.vaArgs.InSilicoVA_keepProbbase_level + ", \n")
                f.write("\t CondProb = " + self.vaArgs.InSilicoVA_CondProb + ", \n")
                f.write("\t CondProbNum = " + self.vaArgs.InSilicoVA_CondProbNum + ", \n")
                f.write("\t datacheck = " + self.vaArgs.InSilicoVA_datacheck + ", \n")
                f.write("\t datacheck.missing = " + self.vaArgs.InSilicoVA_datacheck_missing + ", \n")
                f.write("\t warning.write = TRUE, \n")
                f.write("\t directory = '" + os.path.join(self.dirOpenVA, self.runDate) + "', \n")
                f.write("\t external.sep = " + self.vaArgs.InSilicoVA_external_sep + ", \n")
                f.write("\t Nsim = " + self.vaArgs.InSilicoVA_Nsim + ", \n")
                f.write("\t thin = " + self.vaArgs.InSilicoVA_thin + ", \n")
                f.write("\t burnin = " + self.vaArgs.InSilicoVA_burnin + ", \n")
                f.write("\t auto.length = " + self.vaArgs.InSilicoVA_auto_length + ", \n")
                f.write("\t conv.csmf = " + self.vaArgs.InSilicoVA_conv_csmf + ", \n")
                f.write("\t jump.scale = " + self.vaArgs.InSilicoVA_jump_scale + ", \n")
                f.write("\t levels.prior = " + self.vaArgs.InSilicoVA_levels_prior + ", \n")
                f.write("\t levels.strength = " + self.vaArgs.InSilicoVA_levels_strength + ", \n")
                f.write("\t trunc.min = " + self.vaArgs.InSilicoVA_trunc_min + ", \n")
                f.write("\t trunc.max = " + self.vaArgs.InSilicoVA_trunc_max + ", \n")
                f.write("\t subpop = " + self.vaArgs.InSilicoVA_subpop + ", \n")
                f.write("\t java_option = '" + self.vaArgs.InSilicoVA_java_option + "', \n")
                f.write("\t seed = " + self.vaArgs.InSilicoVA_seed + ", \n")
                f.write("\t phy.code = " + self.vaArgs.InSilicoVA_phy_code + ", \n")
                f.write("\t phy.cat = " + self.vaArgs.InSilicoVA_phy_cat + ", \n")
                f.write("\t phy.unknown = " + self.vaArgs.InSilicoVA_phy_unknown + ", \n")
                f.write("\t phy.external = " + self.vaArgs.InSilicoVA_phy_external + ", \n")
                f.write("\t phy.debias = " + self.vaArgs.InSilicoVA_phy_debias + ", \n")
                f.write("\t exclude.impossible.cause = '" + self.vaArgs.InSilicoVA_exclude_impossible_cause + "', \n")
                f.write("\t no.is.missing = " + self.vaArgs.InSilicoVA_no_is_missing + ", \n")
                f.write("\t indiv.CI = " + self.vaArgs.InSilicoVA_indiv_CI + ", \n")
                f.write("\t groupcode = " + self.vaArgs.InSilicoVA_no_is_missing + ") \n")
                if self.vaArgs.InSilicoVA_data_type == "WHO2012":
                    f.write("sex <- ifelse(tolower(records_sorted$MALE)=='y', 'Male', 'Female') \n")
                if self.vaArgs.InSilicoVA_data_type == "WHO2016":
                    f.write("sex <- ifelse(tolower(records_sorted$i019a)=='y', 'Male', 'Female') \n")
                f.write("cod <- getTopCOD(results) \n")
                f.write("names(cod) <- toupper(names(cod)) \n")
                f.write("dob <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10021), '%Y-%m-%d') \n")
                f.write("dod <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10023), '%Y-%m-%d') \n")
                f.write("age <- floor(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.ageInDays/365.25) \n")
                f.write("## create matrices for DHIS2 blob (records2) and transfer database (records3) \n")
                f.write("## first column must be ID \n")
                f.write("metadataCode <- '" + self.pipelineArgs.algorithmMetadataCode + "'\n")
                f.write("records2 <- merge(cod, records_sorted, by = 'ID', all = TRUE, sort = TRUE) \n")
                f.write("records2[is.na(records2[, 'CAUSE']), 'CAUSE'] <- 'MISSING' \n")
                f.write("cod2 <- records2[, 'CAUSE'] \n")
                f.write("names(records2)[which(names(records2) == 'CAUSE')] <- 'Cause of Death' \n")
                f.write("records2$Metadata <- metadataCode \n")
                f.write("evaBlob <- cbind(rep(as.character(records2[,'ID']), each=ncol(records2)), rep(names(records2)), c(apply(records2, 1, c))) \n")
                f.write("colnames(evaBlob) <- c('ID', 'Attribute', 'Value') \n")
                f.write("write.csv(evaBlob, file='" + self.dirOpenVA + "/entityAttributeValue.csv', row.names=FALSE, na='') \n\n")
                f.write("records3 <- cbind(as.character(records_sorted[,'ID']), sex, dob, dod, age, cod2, metadataCode, raw_data_sorted$meta.instanceID, records_sorted[,-1]) \n")
                f.write("names(records3) <- c('id', 'sex', 'dob', 'dod', 'age', 'cod', 'metadataCode', 'odkMetaInstanceID', names(records_sorted[,-1])) \n")
                f.write("write.csv(records3, file='" + self.dirOpenVA + "/recordStorage.csv', row.names=FALSE, na='') \n")
                f.write("date() \n")
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Problem writing R script " +
                              "for InSilicoVA.") from exc

    def _rScript_interVA(self):

        fileName = os.path.join(self.dirOpenVA,
                                self.runDate,
                                "Rscript_" + self.runDate + ".R")

        algorithmMetadata = self.pipelineArgs.algorithmMetadataCode.split("|")
        whoInstrumentVersion = algorithmMetadata[5]
        raw_data_file = os.path.join(self.dirOpenVA, "pycrossva_input.csv")

        if whoInstrumentVersion not in ["v1_4_1", "v1_5_1", "v1_5_3"]:
            raise OpenVAError("pyCrossVA not able to process WHO " +
                              "instrument version: " + whoInstrumentVersion)
        try:
            with open(fileName, "w", newline="") as f:
                f.write("date() \n")
                f.write("library(openVA) \n")
                f.write("getwd() \n")
                f.write("raw_data <- read.csv('" + raw_data_file + "') \n")
                odk_id_for_r = self.odkID.replace("-", ".")
                f.write("raw_data_sorted <- raw_data[order(raw_data$" + odk_id_for_r + "),] \n")
                f.write("data_from_pycrossva <- read.csv('" + self.dirOpenVA + "/openVA_input.csv') \n")
                f.write("records_sorted <- data_from_pycrossva[order(data_from_pycrossva$ID),] \n")
                if self.vaArgs.InterVA_Version == "4":
                    f.write("results <- InterVA(Input = records_sorted, \n")
                else:
                    f.write("results <- InterVA5(Input = records_sorted, \n")
                f.write("\t HIV = '" + self.vaArgs.InterVA_HIV + "', \n")
                f.write("\t Malaria = '" + self.vaArgs.InterVA_Malaria + "', \n")
                f.write("\t output = '" + self.vaArgs.InterVA_output + "', \n")
                if self.vaArgs.InterVA_Version == "4":
                    f.write("\t replicate = " + self.vaArgs.InterVA_replicate + ", \n")
                    f.write("\t replicate.bug1 = " + self.vaArgs.InterVA_replicate_bug1 + ", \n")
                    f.write("\t replicate.bug2 = " + self.vaArgs.InterVA_replicate_bug2 + ", \n")
                f.write("\t groupcode = " + self.vaArgs.InterVA_groupcode + ", \n")
                f.write("\t write = TRUE, \n")
                f.write("\t directory = '" + os.path.join(self.dirOpenVA, self.runDate) + "', \n")
                f.write("\t filename = 'warnings_" + self.runDate + "') \n")
                if self.vaArgs.InterVA_Version == "4":
                    f.write("sex <- ifelse(tolower(records_sorted$MALE)=='y', 'Male', 'Female') \n")
                if self.vaArgs.InterVA_Version == "5":
                    f.write("sex <- ifelse(tolower(records_sorted$i019a)=='y', 'Male', 'Female') \n")
                f.write("cod <- getTopCOD(results) \n")
                f.write("names(cod) <- toupper(names(cod)) \n")
                f.write("dob <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10021), '%Y-%m-%d') \n")
                f.write("dod <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10023), '%Y-%m-%d') \n")
                f.write("age <- floor(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.ageInDays/365.25) \n")
                f.write("## create matrices for DHIS2 blob (data2) and transfer database (data3) \n")
                f.write("## first column must be ID \n")
                f.write("metadataCode <- '" + self.pipelineArgs.algorithmMetadataCode + "'\n")
                f.write("records2 <- merge(cod, records_sorted, by = 'ID', all = TRUE, sort = TRUE) \n")
                f.write("records2[is.na(records2[, 'CAUSE']), 'CAUSE'] <- 'MISSING' \n")
                f.write("cod2 <- records2[, 'CAUSE'] \n")
                f.write("names(records2)[which(names(records2) == 'CAUSE')] <- 'Cause of Death' \n")
                f.write("records2$Metadata <- metadataCode \n")
                f.write("evaBlob <- cbind(rep(as.character(records2[,'ID']), each=ncol(records2)), rep(names(records2)), c(apply(records2, 1, c))) \n")
                f.write("colnames(evaBlob) <- c('ID', 'Attribute', 'Value') \n")
                f.write("write.csv(evaBlob, file='" + self.dirOpenVA + "/entityAttributeValue.csv', row.names=FALSE, na='') \n\n")
                f.write("records3 <- cbind(as.character(records_sorted[,'ID']), sex, dob, dod, age, cod2, metadataCode, raw_data_sorted$meta.instanceID, records_sorted[,-1]) \n")
                f.write("names(records3) <- c('id', 'sex', 'dob', 'dod', 'age', 'cod', 'metadataCode', 'odkMetaInstanceID', names(records_sorted[,-1])) \n")
                f.write("write.csv(records3, file='" + self.dirOpenVA + "/recordStorage.csv', row.names=FALSE, na='') \n")
                f.write("date() \n")
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Problem writing R script for InterVA.") from exc

    def smartVA_to_csv(self):
        """Write two CSV files: (1) Entity Value Attribute blob pushed to
                                    DHIS2 (entityAttributeValue.csv)
                                (2) table for transfer database
                                    (recordStorage.csv)

           Both CSV files are stored in the OpenVA folder.
        """

        inFile = os.path.join(self.dirOpenVA, "openVA_input.csv")
        outDir = os.path.join(self.dirOpenVA, self.runDate)
        dfData = read_csv(inFile)
        dfResults = read_csv(outDir +
                             "/1-individual-cause-of-death/" +
                             "individual-cause-of-death.csv")
        codeDF = DataFrame(
            np.repeat(self.pipelineArgs.algorithmMetadataCode,
                      dfResults.shape[0]), columns=["metadataCode"]
        )
        dfResults = concat([dfResults, codeDF], axis=1)
        colsKeep = ["sex", "birth_date", "death_date",
                    "age", "cause34", "metadataCode", "sid"]
        dfRecordStorage = merge(left=dfResults[colsKeep],
                                left_on="sid",
                                right=dfData,
                                right_on="Generalmodule-sid",
                                how="right")
        dfRecordStorage.rename(columns={"meta-instanceID":
                                        "odkMetaInstanceID"},
                               inplace=True)
        dfRecordStorage.drop(columns="sid", inplace=True)
        dfRecordStorage.insert(loc=0, column="ID",
                               value=dfRecordStorage["odkMetaInstanceID"])
        dfRecordStorage.to_csv(self.dirOpenVA + "/recordStorage.csv",
                               index=False)

        colsKeep = ["sid", "cause34", "metadataCode"]
        dfTemp = merge(left=dfResults[colsKeep],
                       left_on="sid",
                       right=dfData,
                       right_on="Generalmodule-sid",
                       how="right")
        dfTemp.dropna(subset=["cause34"])
        dfTemp.drop(columns="sid", inplace=True)
        dfTemp.rename(columns={"meta-instanceID": "odkMetaInstanceID"},
                      inplace=True)
        dfTemp["ID"] = dfTemp["odkMetaInstanceID"]
        dfEVA = dfTemp.melt(id_vars=["ID"],
                            var_name="Attribute",
                            value_name="Value")
        dfEVA.sort_values(by=["ID"], inplace=True)
        dfEVA.to_csv(self.dirOpenVA + "/entityAttributeValue.csv", index=False)

    def getCOD(self):
        """Create and execute R script to assign a COD with openVA; or call
           the SmartVA CLI to assign COD."""

        if self.pipelineArgs.algorithm in ["InSilicoVA", "InterVA"]:
            rScriptIn = os.path.join(self.dirOpenVA, self.runDate,
                                     "Rscript_" + self.runDate + ".R")
            rScriptOut = os.path.join(self.dirOpenVA, self.runDate,
                                      "Rscript_" + self.runDate + ".Rout")
            rArgs = ["R", "CMD", "BATCH", "--vanilla",
                     rScriptIn, rScriptOut]
            try:
                # capture_output=True not available in Python 3.6
                completed = subprocess.run(args=rArgs,
                                           stdin=subprocess.PIPE,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE,
                                           check=True)
            except subprocess.CalledProcessError as exc:
                if exc.returncode == 1:
                    self.successfulRun = False
                    raise OpenVAError("Error running R script:" +
                                      str(exc.stderr)) from exc
            self.successfulRun = True
            return completed
        # if not ["InSilicoVA", "InterVA"], then run SmartVA
        try:
            os.makedirs(
                os.path.join(self.dirOpenVA, self.runDate)
            )
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Unable to create openVA dir" +
                              str(exc)) from exc

        inFile = os.path.join(self.dirOpenVA, "openVA_input.csv")
        outDir = os.path.join(self.dirOpenVA, self.runDate)
        svaArgs = [self.cliSmartVA,
                   "--country", "{}".format(self.vaArgs.SmartVA_country),
                   "--hiv", "{}".format(self.vaArgs.SmartVA_hiv),
                   "--malaria", "{}".format(self.vaArgs.SmartVA_malaria),
                   "--hce", "{}".format(self.vaArgs.SmartVA_hce),
                   "--freetext", "{}".format(self.vaArgs.SmartVA_freetext),
                   "--figures", "{}".format(self.vaArgs.SmartVA_figures),
                   "--language", "{}".format(self.vaArgs.SmartVA_language),
                   inFile,
                   outDir]
        try:
            # capture_output=True not available in Python 3.6
            completed = subprocess.run(args=svaArgs,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       check=True)
        except subprocess.CalledProcessError as exc:
            if exc.returncode == 2:
                self.successfulRun = False
                raise SmartVAError("Error running SmartVA:" +
                                   str(exc.stderr)) from exc
            if "Country list" in exc.stdout:
                self.successfulRun = False
                raise SmartVAError("Problem with SmartVA " +
                                   "country code") from exc

        self.smartVA_to_csv()
        self.successfulRun = True
        return completed
