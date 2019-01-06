"""
openva_pipeline.openVA
----------------------

This module runs openVA and smartVA to assign causes of death to VA records.
"""

import subprocess
import shutil
import os
import pandas as pd
import numpy as np
from pysqlcipher3 import dbapi2 as sqlcipher

from .exceptions import PipelineError
from .exceptions import OpenVAError
from .exceptions import SmartVAError

class OpenVA:
    """Assign cause of death (COD) to verbal autopsies (VA) R package openVA.

    This class creates and executes an R script that copies (and merges) ODK Briefcase
    exports, runs openVA to assign CODs, and creates outputs for depositing in
    the Transfers DB and to a DHIS server.

    :param algorithm: Which VA algorithm should be used to assign COD.
    :type algorithm: str
    :raises: OpenVAError
    """

    def __init__(self, vaArgs, pipelineArgs, odkID, runDate):

        self.vaArgs = vaArgs
        self.pipelineArgs = pipelineArgs
        self.odkID = odkID
        self.runDate = runDate

        dirOpenVA = os.path.join(pipelineArgs.workingDirectory, "OpenVAFiles")
        self.dirOpenVA = dirOpenVA
        dirODK = os.path.join(pipelineArgs.workingDirectory, "ODKFiles")
        self.dirODK = dirODK

        self.cliSmartVA = os.path.join(pipelineArgs.workingDirectory, "smartva")
        self.successfulRun = None

        try:
            if not os.path.isdir(dirOpenVA):
                os.makedirs(dirOpenVA)
        except:
            raise OpenVAError("Unable to create directory" + dhisPath)

    def copyVA(self): 
        """Create data file for openVA by merging ODK export files.

        :returns: Indicator of an empty (i.e. no records) ODK export file
        :rtype: logical 
        """

        exportFile_prev = os.path.join(self.dirODK, "odkBCExportPrev.csv")
        exportFile_new = os.path.join(self.dirODK, "odkBCExportNew.csv")
        openVAInputFile = os.path.join(self.dirOpenVA, "openVA_input.csv")

        isExportFile_prev = os.path.isfile(exportFile_prev)
        isExportFile_new = os.path.isfile(exportFile_new)

        zeroRecords = False

        if isExportFile_new and not isExportFile_prev:
            with open(exportFile_new, "r", newline = "") as fNew:
                fNewLines = fNew.readlines()
            if len(fNewLines) == 1:
                zeroRecords = True
                return(zeroRecords)
            else:
                shutil.copy(exportFile_new, openVAInputFile)

        if isExportFile_new and isExportFile_prev:
            with open(exportFile_new, "r", newline = "") as fNew:
                fNewLines = fNew.readlines()
            with open(exportFile_prev, "r", newline = "") as fPrev:
                fPrevLines = fPrev.readlines()

            if len(fNewLines) == 1 and len(fPrevLines) == 1:
                zeroRecords = True
                return(zeroRecords)

            else:
                shutil.copy(exportFile_new, openVAInputFile)
                with open(openVAInputFile, "a", newline = "") as fCombined:
                    for line in fPrevLines:
                        if line not in fNewLines:
                            fCombined.write(line)
                return(zeroRecords)

    def rScript(self):
        """Create an R script for running openVA and assigning CODs."""

        if not self.pipelineArgs.algorithm == "SmartVA":
            try:
                os.makedirs(
                    os.path.join(self.dirOpenVA, self.runDate)
                )
            except PermissionError as e:
                raise OpenVAError("Unable to create openVA dir" + str(e))
        
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
        rCode_crossVA = "data <- map_records_insilicova(records) \n"

        if whoInstrumentVersion not in ["v1_4_1"]:
            raise OpenVAError \
                ("CrossVA not able to process WHO instrument version: " +
                 whoInstrumentVersion)

        if whoInstrumentVersion == "v1_4_1" and self.vaArgs.InSilicoVA_data_type == "WHO2016":
            rCode_crossVA = "data <- odk2openVA(records) \n"
            # rCode_crossVA = "data <- odk2openVA_v141(records) \n"
        # if whoInstrumentVersion == "v1_5_1" and self.vaArgs.InSilicoVA_data_type == "WHO2016":
        #     rCode_crossVA = "data <- odk2openVA_v151(records) \n"
        try:
            with open(fileName, "w", newline = "") as f:
                f.write("date() \n")
                f.write("library(openVA); library(CrossVA) \n")
                f.write("getwd() \n")
                f.write("records <- read.csv('" + self.dirOpenVA + "/openVA_input.csv') \n")
                f.write(rCode_crossVA)
                f.write("names(data) <- tolower(names(data)) \n")
                f.write("odkMetaInstanceID <- as.character(records$meta.instanceID) \n")
                if self.odkID == None:
                    f.write("data$id <- odkMetaInstanceID \n")
                else: 
                    f.write("data$id <- as.character(records$" + self.odkID + ")\n")
                f.write("results <- insilico(data = data, \n")
                f.write("\t data.type = '" + self.vaArgs.InSilicoVA_data_type + "', \n")
                f.write("\t isNumeric = " + self.vaArgs.InSilicoVA_isNumeric + ", \n")
                f.write("\t updateCondProb = " + self.vaArgs.InSilicoVA_updateCondProb + ", \n")
                f.write("\t keepProbbase.level = " + self.vaArgs.InSilicoVA_keepProbbase_level + ", \n")
                f.write("\t CondProb = " + self.vaArgs.InSilicoVA_CondProb + ", \n")
                f.write("\t CondProbNum = " + self.vaArgs.InSilicoVA_CondProbNum + ", \n")
                f.write("\t datacheck = " + self.vaArgs.InSilicoVA_datacheck + ", \n")
                f.write("\t datacheck.missing = " + self.vaArgs.InSilicoVA_datacheck_missing + ", \n")
                # f.write("\t warning.write = " + self.vaArgs.InSilicoVA_warning_write + ", \n")
                # f.write("\t directory = " + self.vaArgs.InSilicoVA_directory + ", \n")
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
                f.write("sex <- ifelse(tolower(data$male)=='y', 'Male', 'Female') \n")
                f.write("cod <- getTopCOD(results) \n")
                f.write("hasCOD <- as.character(data$id) %in% as.character(levels(cod$ID)) \n")
                f.write("dob <- as.Date(as.character(records$consented.deceased_CRVS.info_on_deceased.Id10021), '%b %d, %Y') \n")
                f.write("dod <- as.Date(as.character(records$consented.deceased_CRVS.info_on_deceased.Id10023), '%b %d, %Y') \n")
                f.write("age <- floor(records$consented.deceased_CRVS.info_on_deceased.ageInDays/365.25) \n")
                f.write("## create matrices for DHIS2 blob (data2) and transfer database (data3) \n")
                f.write("## first column must be ID \n")
                f.write("metadataCode <- '" + self.pipelineArgs.algorithmMetadataCode + "'\n")
                f.write("cod2 <- rep('MISSING', nrow(data)); cod2[hasCOD] <- as.character(cod[,2]) \n")
                f.write("data2 <- cbind(data[,-1], cod2, metadataCode) \n")
                f.write("names(data2) <- c(names(data[,-1]), 'Cause of Death', 'Metadata') \n")
                f.write("evaBlob <- cbind(rep(as.character(data[,1]), each=ncol(data2)), rep(names(data2)), c(apply(data2, 1, c))) \n")
                f.write("colnames(evaBlob) <- c('ID', 'Attribute', 'Value') \n")
                f.write("write.csv(evaBlob, file='" + self.dirOpenVA + "/entityAttributeValue.csv', row.names=FALSE, na='') \n\n")
                f.write("data3 <- cbind(as.character(data[,1]), sex, dob, dod, age, cod2, metadataCode, odkMetaInstanceID, data[,-1]) \n")
                f.write("names(data3) <- c('id', 'sex', 'dob', 'dod', 'age', 'cod', 'metadataCode', 'odkMetaInstanceID', names(data[,-1])) \n")
                f.write("write.csv(data3, file='" + self.dirOpenVA + "/recordStorage.csv', row.names=FALSE, na='') \n")
        except:
            raise OpenVAError("Problem writing R script for InSilicoVA.")

    def _rScript_interVA(self):

        fileName = os.path.join(self.dirOpenVA,
                                self.runDate,
                                "Rscript_" + self.runDate + ".R")

        algorithmMetadata = self.pipelineArgs.algorithmMetadataCode.split("|")
        whoInstrumentVersion = algorithmMetadata[5]
        rCode_crossVA = "data <- map_records_interva4(records) \n"

        if whoInstrumentVersion not in ["v1_4_1"]:
            raise OpenVAError \
                ("CrossVA not able to process WHO instrument version: " +
                 whoInstrumentVersion)

        if whoInstrumentVersion == "v1_4_1" and self.vaArgs.InterVA_Version == "5":
            rCode_crossVA = "data <- odk2openVA(records) \n"
            # rCode_crossVA = "data <- odk2openVA_v141(records) \n"
        # if whoInstrumentVersion == "v1_5_1" and self.vaArgs.InterVA_Version == "5":
        #     rCode_crossVA = "data <- odk2openVA_v151(records) \n"
        try:
            with open(fileName, "w", newline = "") as f:
                f.write("date() \n")
                f.write("library(openVA); library(CrossVA) \n")
                f.write("getwd() \n")
                f.write("records <- read.csv('" + self.dirOpenVA + "/openVA_input.csv') \n")
                f.write(rCode_crossVA)
                f.write("odkMetaInstanceID <- as.character(records$meta.instanceID) \n")
                if self.odkID == None:
                    f.write("data$ID <- odkMetaInstanceID \n")
                else: 
                    f.write("data$ID <- as.character(records$" + self.odkID + ")\n")
                if self.vaArgs.InterVA_Version == "4":
                    f.write("results <- InterVA(Input = data, \n")
                else:
                    f.write("results <- InterVA5(Input = data, \n")
                f.write("\t HIV = '" + self.vaArgs.InterVA_HIV + "', \n")
                f.write("\t Malaria = '" + self.vaArgs.InterVA_Malaria + "', \n")
                f.write("\t output = '" + self.vaArgs.InterVA_output + "', \n")
                if self.vaArgs.InterVA_Version == "4":
                    f.write("\t replicate = " + self.vaArgs.InterVA_replicate + ", \n")
                    f.write("\t replicate.bug1 = " + self.vaArgs.InterVA_replicate_bug1 + ", \n")
                    f.write("\t replicate.bug2 = " + self.vaArgs.InterVA_replicate_bug2 + ", \n")
                f.write("\t groupcode = " + self.vaArgs.InterVA_groupcode + ", \n")
                # f.write("\t write = " + self.vaArgs.InterVA_write + ", \n")
                # f.write("\t directory = '" + self.vaArgs.InterVA_directory + "', \n")
                # f.write("\t filename = '" +
                #         self.vaArgs.InterVA_filename + "_" + self.runDate + "') \n")
                f.write("\t write = TRUE, \n")
                f.write("\t directory = '" + os.path.join(self.dirOpenVA, self.runDate) + "', \n")
                f.write("\t filename = 'warnings_" + self.runDate + "') \n")
                f.write("sex <- ifelse(tolower(data$MALE) == 'y', 'Male', 'Female') \n")
                f.write("cod <- getTopCOD(results) \n")
                f.write("hasCOD <- as.character(data$ID) %in% as.character(levels(cod$ID)) \n")
                f.write("dob <- as.Date(as.character(records$consented.deceased_CRVS.info_on_deceased.Id10021), '%b %d, %Y') \n")
                f.write("dod <- as.Date(as.character(records$consented.deceased_CRVS.info_on_deceased.Id10023), '%b %d, %Y') \n")
                f.write("age <- floor(records$consented.deceased_CRVS.info_on_deceased.ageInDays/365.25) \n")
                f.write("## create matrices for DHIS2 blob (data2) and transfer database (data3) \n")
                f.write("## first column must be ID \n")
                f.write("metadataCode <- '" + self.pipelineArgs.algorithmMetadataCode + "'\n")
                f.write("cod2 <- rep('MISSING', nrow(data)); cod2[hasCOD] <- as.character(cod[,2]) \n")
                f.write("data2 <- cbind(data[,-1], cod2, metadataCode) \n")
                f.write("names(data2) <- c(names(data[,-1]), 'Cause of Death', 'Metadata') \n")
                f.write("evaBlob <- cbind(rep(as.character(data[,1]), each=ncol(data2)), rep(names(data2)), c(apply(data2, 1, c))) \n")
                f.write("colnames(evaBlob) <- c('ID', 'Attribute', 'Value') \n")
                f.write("write.csv(evaBlob, file='" + self.dirOpenVA + "/entityAttributeValue.csv', row.names=FALSE, na='') \n\n")
                f.write("data3 <- cbind(as.character(data[,1]), sex, dob, dod, age, cod2, metadataCode, odkMetaInstanceID, data[,-1]) \n")
                f.write("names(data3) <- c('id', 'sex', 'dob', 'dod', 'age', 'cod', 'metadataCode', 'odkMetaInstanceID', names(data[,-1])) \n")
                f.write("write.csv(data3, file='" + self.dirOpenVA + "/recordStorage.csv', row.names=FALSE, na='') \n")
        except:
            raise OpenVAError("Problem writing R script for InterVA.")

    def smartVA_to_csv(self):
        """Write two CSV files: (1) Entity Value Attribute blob pushed to DHIS2 (entityAttributeValue.csv)
                                (2) table for transfer database (recordStorage.csv)

           Both CSV files are stored in the OpenVA folder.
        """

        inFile = os.path.join(self.dirOpenVA, "openVA_input.csv")
        outDir = os.path.join(self.dirOpenVA, self.runDate)
        dfData    = pd.read_csv(inFile)
        dfResults = pd.read_csv(outDir +
                    "/1-individual-cause-of-death/individual-cause-of-death.csv")
        codeDF    = pd.DataFrame(
            np.repeat(self.pipelineArgs.algorithmMetadataCode,
                      dfResults.shape[0]), columns=["metadataCode"]
        )
        dfResults = pd.concat([dfResults, codeDF], axis=1)
        colsKeep = ["sex", "birth_date", "death_date",
                    "age", "cause34", "metadataCode", "sid"]
        dfRecordStorage = pd.merge(left=dfResults[colsKeep],
                                   left_on="sid",
                                   right=dfData,
                                   right_on="Generalmodule-sid",
                                   how="right")
        dfRecordStorage.rename(columns =
                               {"meta-instanceID": "odkMetaInstanceID"},
                               inplace = True
        )
        dfRecordStorage.drop(columns="sid", inplace = True)
        dfRecordStorage.insert(loc=0, column="ID",
                               value=dfRecordStorage["odkMetaInstanceID"])
        dfRecordStorage.to_csv(self.dirOpenVA + "/recordStorage.csv", index = False)

        colsKeep = ["sid", "cause34", "metadataCode"]
        dfTemp   = pd.merge(left=dfResults[colsKeep],
                            left_on="sid",
                            right=dfData,
                            right_on="Generalmodule-sid",
                            how="right")
        dfTemp.dropna(subset=["cause34"])
        dfTemp.drop(columns="sid", inplace=True)
        dfTemp.rename(columns = {"meta-instanceID": "odkMetaInstanceID"},
                      inplace = True
        )
        dfTemp["ID"] = dfTemp["odkMetaInstanceID"]
        dfEVA = dfTemp.melt(id_vars=["ID"],
                            var_name="Attribute",
                            value_name="Value")
        dfEVA.sort_values(by=["ID"], inplace=True)
        dfEVA.to_csv(self.dirOpenVA + "/entityAttributeValue.csv", index=False)

    def getCOD(self):
        """Create and execute R script to assign a COD with openVA; or call the SmartVA CLI to assign COD."""

        if self.pipelineArgs.algorithm in ["InSilicoVA", "InterVA"]:
            rScriptIn = os.path.join(self.dirOpenVA, self.runDate,
                                     "Rscript_" + self.runDate + ".R")
            rScriptOut = os.path.join(self.dirOpenVA, self.runDate,
                                      "Rscript_" + self.runDate + ".Rout")
            rArgs = ["R", "CMD", "BATCH", "--vanilla",
                     rScriptIn, rScriptOut]
            completed = subprocess.run(args = rArgs,
                                       stdin  = subprocess.PIPE,
                                       stdout = subprocess.PIPE,
                                       stderr = subprocess.PIPE)
            if completed.returncode == 1:
                self.successfulRun = False
                raise OpenVAError("Error running R script:" + str(completed.stderr))
            self.successfulRun = True
            return(completed)

        if self.pipelineArgs.algorithm == "SmartVA":
            try:
                os.makedirs(
                    os.path.join(self.dirOpenVA, self.runDate)
                )
            except PermissionError as e:
                raise OpenVAError("Unable to create openVA dir" + str(e))

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

            completed = subprocess.run(args = svaArgs,
                                       stdin  = subprocess.PIPE,
                                       stdout = subprocess.PIPE,
                                       stderr = subprocess.PIPE)
            if completed.returncode == 2:
                self.successfulRun = False
                raise SmartVAError \
                    ("Error running SmartVA:" + str(completed.stderr))
            stdOut = str(completed.stdout)
            if "Country list" in stdOut:
                self.successfulRun = False
                raise SmartVAError("Problem with SmartVA country code")

            self.smartVA_to_csv()
            self.successfulRun = True
            return(completed)

#------------------------------------------------------------------------------#
# Exceptions
#------------------------------------------------------------------------------#

