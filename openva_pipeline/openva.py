"""
openva_pipeline.openva
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

    :param settings: Configuration settings for pipeline steps (which is
    returned from :meth:`Pipeline.config() <config>`).
    :type settings: dictionary of named tuples
    :param pipeline_run_date: Date and time when instance of
    :class:`Pipeline <Pipeline>` was created (instance attribute).
    :type pipeline_run_date: datetime.datetime.now() with formatting
    strftime("%Y-%m-%d_%H:%M:%S")
    :raises: OpenVAError
    """

    def __init__(self, settings, pipeline_run_date):

        self.va_args = settings["openva"]
        self.pipeline_args = settings["pipeline"]
        if settings["odk"].odk_id is None:
            self.odk_id = "meta-instanceID"
        else:
            self.odk_id = settings["odk"].odk_id
        self.run_date = pipeline_run_date

        dir_openva = os.path.join(self.pipeline_args.working_directory,
                                  "OpenVAFiles")
        self.dir_openva = dir_openva
        dir_odk = os.path.join(self.pipeline_args.working_directory,
                               "ODKFiles")
        self.dir_odk = dir_odk

        self.cli_smartva = os.path.join(self.pipeline_args.working_directory,
                                        "smartva")
        self.successful_run = None

        try:
            if not os.path.isdir(dir_openva):
                os.makedirs(dir_openva)
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Unable to create directory" +
                              dir_openva) from exc

    def copy_va(self):
        """Create data file for openVA by merging ODK export files & converting
           with pycrossva.

        :returns: Indicator of an empty (i.e. no records) ODK export file
        :rtype: logical
        """

        export_file_prev = os.path.join(self.dir_odk, "odk_bc_export_prev.csv")
        export_file_new = os.path.join(self.dir_odk, "odk_bc_export_new.csv")
        pycva_input = os.path.join(self.dir_openva, "pycrossva_input.csv")
        openva_input_file = os.path.join(self.dir_openva, "openVA_input.csv")

        is_export_file_prev = os.path.isfile(export_file_prev)
        is_export_file_new = os.path.isfile(export_file_new)

        zero_records = False

        algorithm_metadata = \
            self.pipeline_args.algorithm_metadata_code.split("|")
        who_instrument_version = algorithm_metadata[5]
        if who_instrument_version == "v1_4_1":
            pcva_input = "2016WHOv141"
        else:
            pcva_input = "2016WHOv151"

        if is_export_file_new and not is_export_file_prev:
            with open(export_file_new, "r", newline="") as f_new:
                f_new_lines = f_new.readlines()
            if len(f_new_lines) == 1:
                zero_records = True
                return zero_records
            shutil.copy(export_file_new, pycva_input)
            if self.pipeline_args.algorithm == "SmartVA":
                shutil.copy(pycva_input, openva_input_file)
            else:
                final_data = transform(mapping=(pcva_input, "InterVA5"),
                                       raw_data=pycva_input,
                                       raw_data_id=self.odk_id,
                                       verbose=0)
                final_data.to_csv(openva_input_file, index=False)

        if is_export_file_new and is_export_file_prev:
            with open(export_file_new, "r", newline="") as f_new:
                f_new_lines = f_new.readlines()
            with open(export_file_prev, "r", newline="") as f_prev:
                f_prev_lines = f_prev.readlines()

            if len(f_new_lines) == 1 and len(f_prev_lines) == 1:
                zero_records = True
                return zero_records

            shutil.copy(export_file_new, pycva_input)
            with open(pycva_input, "a", newline="") as fCombined:
                for line in f_prev_lines:
                    if line not in f_new_lines:
                        fCombined.write(line)
            if self.pipeline_args.algorithm == "SmartVA":
                shutil.copy(pycva_input, openva_input_file)
            else:
                final_data = transform(mapping=(pcva_input, "InterVA5"),
                                       raw_data=pycva_input,
                                       raw_data_id=self.odk_id,
                                       verbose=0)
                final_data.to_csv(openva_input_file, index=False)
        return zero_records

    def r_script(self):
        """Create an R script for running openVA and assigning CODs."""

        if not self.pipeline_args.algorithm == "SmartVA":
            try:
                os.makedirs(
                    os.path.join(self.dir_openva, self.run_date)
                )
            except (PermissionError, OSError) as exc:
                raise OpenVAError("Unable to create openVA dir" +
                                  str(exc)) from exc

        if self.pipeline_args.algorithm == "InSilicoVA":
            self._r_script_insilicova()
        if self.pipeline_args.algorithm == "InterVA":
            self._r_script_interva()

    def _r_script_insilicova(self):

        file_name = os.path.join(self.dir_openva,
                                 self.run_date,
                                 "r_script_" + self.run_date + ".R")
        algorithm_metadata = \
            self.pipeline_args.algorithm_metadata_code.split("|")
        who_instrument_version = algorithm_metadata[5]
        raw_data_file = os.path.join(self.dir_openva, "pycrossva_input.csv")

        if who_instrument_version not in ["v1_4_1", "v1_5_1", "v1_5_3"]:
            raise OpenVAError("pyCrossVA not able to process WHO " +
                              "instrument version: " + who_instrument_version)
        try:
            with open(file_name, "w", newline="") as f:
                f.write("date() \n")
                f.write("library(openVA) \n")
                f.write("getwd() \n")
                f.write("raw_data <- read.csv('" + raw_data_file + "') \n")
                odk_id_for_r = self.odk_id.replace("-", ".")
                f.write("raw_data_sorted <- raw_data[order(raw_data$" + odk_id_for_r + "),] \n")
                f.write("data_from_pycrossva <- read.csv('" + self.dir_openva + "/openVA_input.csv') \n")
                f.write("records_sorted <- data_from_pycrossva[order(data_from_pycrossva$ID),] \n")
                f.write("results <- insilico(data = records_sorted, \n")
                f.write("\t data.type = '" + self.va_args.insilicova_data_type + "', \n")
                f.write("\t isNumeric = " + self.va_args.insilicova_is_numeric + ", \n")
                f.write("\t updateCondProb = " + self.va_args.insilicova_update_cond_prob + ", \n")
                f.write("\t keepProbbase.level = " + self.va_args.insilicova_keep_probbase_level + ", \n")
                f.write("\t CondProb = " + self.va_args.insilicova_cond_prob + ", \n")
                f.write("\t CondProbNum = " + self.va_args.insilicova_cond_prob_num + ", \n")
                f.write("\t datacheck = " + self.va_args.insilicova_datacheck + ", \n")
                f.write("\t datacheck.missing = " + self.va_args.insilicova_datacheck_missing + ", \n")
                f.write("\t warning.write = TRUE, \n")
                f.write("\t directory = '" + os.path.join(self.dir_openva, self.run_date) + "', \n")
                f.write("\t external.sep = " + self.va_args.insilicova_external_sep + ", \n")
                f.write("\t Nsim = " + self.va_args.insilicova_nsim + ", \n")
                f.write("\t thin = " + self.va_args.insilicova_thin + ", \n")
                f.write("\t burnin = " + self.va_args.insilicova_burnin + ", \n")
                f.write("\t auto.length = " + self.va_args.insilicova_auto_length + ", \n")
                f.write("\t conv.csmf = " + self.va_args.insilicova_conv_csmf + ", \n")
                f.write("\t jump.scale = " + self.va_args.insilicova_jump_scale + ", \n")
                f.write("\t levels.prior = " + self.va_args.insilicova_levels_prior + ", \n")
                f.write("\t levels.strength = " + self.va_args.insilicova_levels_strength + ", \n")
                f.write("\t trunc.min = " + self.va_args.insilicova_trunc_min + ", \n")
                f.write("\t trunc.max = " + self.va_args.insilicova_trunc_max + ", \n")
                f.write("\t subpop = " + self.va_args.insilicova_subpop + ", \n")
                f.write("\t java_option = '" + self.va_args.insilicova_java_option + "', \n")
                f.write("\t seed = " + self.va_args.insilicova_seed + ", \n")
                f.write("\t phy.code = " + self.va_args.insilicova_phy_code + ", \n")
                f.write("\t phy.cat = " + self.va_args.insilicova_phy_cat + ", \n")
                f.write("\t phy.unknown = " + self.va_args.insilicova_phy_unknown + ", \n")
                f.write("\t phy.external = " + self.va_args.insilicova_phy_external + ", \n")
                f.write("\t phy.debias = " + self.va_args.insilicova_phy_debias + ", \n")
                f.write("\t exclude.impossible.cause = '" + self.va_args.insilicova_exclude_impossible_cause + "', \n")
                f.write("\t no.is.missing = " + self.va_args.insilicova_no_is_missing + ", \n")
                f.write("\t indiv.CI = " + self.va_args.insilicova_indiv_ci + ", \n")
                f.write("\t groupcode = " + self.va_args.insilicova_no_is_missing + ") \n")
                if self.va_args.insilicova_data_type == "WHO2012":
                    f.write("sex <- ifelse(tolower(records_sorted$MALE)=='y', 'Male', 'Female') \n")
                if self.va_args.insilicova_data_type == "WHO2016":
                    f.write("sex <- ifelse(tolower(records_sorted$i019a)=='y', 'Male', 'Female') \n")
                f.write("cod <- getTopCOD(results) \n")
                f.write("names(cod) <- toupper(names(cod)) \n")
                f.write("dob <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10021), '%Y-%m-%d') \n")
                f.write("if (length(dob) == 0) { \n")
                f.write("  index_id10021 <- which(grepl('id10021$', tolower(names(raw_data_sorted))))\n")
                f.write("  dob <- as.Date(as.character(raw_data_sorted[, index_id10021]), '%Y-%m-%d')}\n")
                f.write("dod <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10023), '%Y-%m-%d') \n")
                f.write("if (length(dod) == 0) { \n")
                f.write("  index_id10023 <- which(grepl('id10023$', tolower(names(raw_data_sorted))))\n")
                f.write("  dod <- as.Date(as.character(raw_data_sorted[, index_id10023]), '%Y-%m-%d')}\n")
                f.write("age <- floor(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.ageInDays/365.25) \n")
                f.write("if (length(age) == 0) { \n")
                f.write("    index_ageindays  <- which(grepl('ageindays$', tolower(names(raw_data_sorted))))\n")
                f.write("    age <- floor(raw_data_sorted[, index_ageindays]/365.25)}\n")
                f.write("## create matrices for DHIS2 blob (records2) and transfer database (records3) \n")
                f.write("## first column must be ID \n")
                f.write("metadataCode <- '" + self.pipeline_args.algorithm_metadata_code + "'\n")
                f.write("records2 <- merge(cod, records_sorted, by = 'ID', all = TRUE, sort = TRUE) \n")
                f.write("records2[is.na(records2[, 'CAUSE']), 'CAUSE'] <- 'MISSING' \n")
                f.write("cod2 <- records2[, 'CAUSE'] \n")
                f.write("names(records2)[which(names(records2) == 'CAUSE')] <- 'Cause of Death' \n")
                f.write("records2$Metadata <- metadataCode \n")
                f.write("evaBlob <- cbind(rep(as.character(records2[,'ID']), each=ncol(records2)), rep(names(records2)), c(apply(records2, 1, c))) \n")
                f.write("colnames(evaBlob) <- c('ID', 'Attribute', 'Value') \n")
                f.write("write.csv(evaBlob, file='" + self.dir_openva + "/entity_attribute_value.csv', row.names=FALSE, na='') \n\n")
                f.write("records3 <- cbind(as.character(records_sorted[,'ID']), sex, dob, dod, age, cod2, metadataCode, raw_data_sorted$meta.instanceID, records_sorted[,-1]) \n")
                f.write("names(records3) <- c('id', 'sex', 'dob', 'dod', 'age', 'cod', 'metadataCode', 'odkMetaInstanceID', names(records_sorted[,-1])) \n")
                f.write("write.csv(records3, file='" + self.dir_openva + "/record_storage.csv', row.names=FALSE, na='') \n")
                f.write("date() \n")
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Problem writing R script " +
                              "for InSilicoVA.") from exc

    def _r_script_interva(self):

        file_name = os.path.join(self.dir_openva,
                                 self.run_date,
                                 "r_script_" + self.run_date + ".R")

        algorithm_metadata = \
            self.pipeline_args.algorithm_metadata_code.split("|")
        who_instrument_version = algorithm_metadata[5]
        raw_data_file = os.path.join(self.dir_openva, "pycrossva_input.csv")

        if who_instrument_version not in ["v1_4_1", "v1_5_1", "v1_5_3"]:
            raise OpenVAError("pyCrossVA not able to process WHO " +
                              "instrument version: " + who_instrument_version)
        try:
            with open(file_name, "w", newline="") as f:
                f.write("date() \n")
                f.write("library(openVA) \n")
                f.write("getwd() \n")
                f.write("raw_data <- read.csv('" + raw_data_file + "') \n")
                odk_id_for_r = self.odk_id.replace("-", ".")
                f.write("raw_data_sorted <- raw_data[order(raw_data$" + odk_id_for_r + "),] \n")
                f.write("data_from_pycrossva <- read.csv('" + self.dir_openva + "/openVA_input.csv') \n")
                f.write("records_sorted <- data_from_pycrossva[order(data_from_pycrossva$ID),] \n")
                if self.va_args.interva_version == "4":
                    f.write("results <- InterVA5(Input = records_sorted, \n")
                else:
                    f.write("results <- interva5(Input = records_sorted, \n")
                f.write("\t HIV = '" + self.va_args.interva_hiv + "', \n")
                f.write("\t Malaria = '" + self.va_args.interva_malaria + "', \n")
                f.write("\t output = '" + self.va_args.interva_output + "', \n")
                if self.va_args.interva_version == "4":
                    f.write("\t replicate = " + self.va_args.interva_replicate + ", \n")
                    f.write("\t replicate.bug1 = " + self.va_args.interva_replicate_bug1 + ", \n")
                    f.write("\t replicate.bug2 = " + self.va_args.interva_replicate_bug2 + ", \n")
                f.write("\t groupcode = " + self.va_args.interva_groupcode + ", \n")
                f.write("\t write = TRUE, \n")
                f.write("\t directory = '" + os.path.join(self.dir_openva, self.run_date) + "', \n")
                f.write("\t filename = 'warnings_" + self.run_date + "') \n")
                if self.va_args.interva_version == "4":
                    f.write("sex <- ifelse(tolower(records_sorted$MALE)=='y', 'Male', 'Female') \n")
                if self.va_args.interva_version == "5":
                    f.write("sex <- ifelse(tolower(records_sorted$i019a)=='y', 'Male', 'Female') \n")
                f.write("cod <- getTopCOD(results) \n")
                f.write("names(cod) <- toupper(names(cod)) \n")
                f.write("dob <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10021), '%Y-%m-%d') \n")
                f.write("if (length(dob) == 0) { \n")
                f.write("  index_id10021 <- which(grepl('id10021$', tolower(names(raw_data_sorted))))\n")
                f.write("  dob <- as.Date(as.character(raw_data_sorted[, index_id10021]), '%Y-%m-%d')}\n")
                f.write("dod <- as.Date(as.character(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.Id10023), '%Y-%m-%d') \n")
                f.write("if (length(dod) == 0) { \n")
                f.write("  index_id10023 <- which(grepl('id10023$', tolower(names(raw_data_sorted))))\n")
                f.write("  dod <- as.Date(as.character(raw_data_sorted[, index_id10023]), '%Y-%m-%d')}\n")
                f.write("age <- floor(raw_data_sorted$consented.deceased_CRVS.info_on_deceased.ageInDays/365.25) \n")
                f.write("if (length(age) == 0) { \n")
                f.write("    index_ageindays  <- which(grepl('ageindays$', tolower(names(raw_data_sorted))))\n")
                f.write("    age <- floor(raw_data_sorted[, index_ageindays]/365.25)}\n")
                f.write("## create matrices for DHIS2 blob (data2) and transfer database (data3) \n")
                f.write("## first column must be ID \n")
                f.write("metadataCode <- '" + self.pipeline_args.algorithm_metadata_code + "'\n")
                f.write("records2 <- merge(cod, records_sorted, by = 'ID', all = TRUE, sort = TRUE) \n")
                f.write("records2[is.na(records2[, 'CAUSE']), 'CAUSE'] <- 'MISSING' \n")
                f.write("cod2 <- records2[, 'CAUSE'] \n")
                f.write("names(records2)[which(names(records2) == 'CAUSE')] <- 'Cause of Death' \n")
                f.write("records2$Metadata <- metadataCode \n")
                f.write("evaBlob <- cbind(rep(as.character(records2[,'ID']), each=ncol(records2)), rep(names(records2)), c(apply(records2, 1, c))) \n")
                f.write("colnames(evaBlob) <- c('ID', 'Attribute', 'Value') \n")
                f.write("write.csv(evaBlob, file='" + self.dir_openva + "/entity_attribute_value.csv', row.names=FALSE, na='') \n\n")
                f.write("records3 <- cbind(as.character(records_sorted[,'ID']), sex, dob, dod, age, cod2, metadataCode, raw_data_sorted$meta.instanceID, records_sorted[,-1]) \n")
                f.write("names(records3) <- c('id', 'sex', 'dob', 'dod', 'age', 'cod', 'metadataCode', 'odkMetaInstanceID', names(records_sorted[,-1])) \n")
                f.write("write.csv(records3, file='" + self.dir_openva + "/record_storage.csv', row.names=FALSE, na='') \n")
                f.write("date() \n")
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Problem writing R script for InterVA.") from exc

    def smartva_to_csv(self):
        """
        Write two CSV files: 
        (1) Entity Value Attribute blob pushed to DHIS2
        (entity_attribute_value.csv)
        (2) table for transfer database (record_storage.csv)

        Both CSV files are stored in the OpenVA folder.
        """

        in_file = os.path.join(self.dir_openva, "openva_input.csv")
        out_dir = os.path.join(self.dir_openva, self.run_date)
        df_data = read_csv(in_file)
        df_results = read_csv(out_dir +
                              "/1-individual-cause-of-death/" +
                              "individual-cause-of-death.csv")
        code_df = DataFrame(
            np.repeat(self.pipeline_args.algorithm_metadata_code,
                      df_results.shape[0]), columns=["metadataCode"]
        )
        df_results = concat([df_results, code_df], axis=1)
        cols_keep = ["sex", "birth_date", "death_date",
                     "age", "cause34", "metadataCode", "sid"]
        df_record_storage = merge(left=df_results[cols_keep],
                                  left_on="sid",
                                  right=df_data,
                                  right_on="Generalmodule-sid",
                                  how="right")
        df_record_storage.rename(columns={"meta-instanceID":
                                 "odkMetaInstanceID"},
                                 inplace=True)
        df_record_storage.drop(columns="sid", inplace=True)
        df_record_storage.insert(loc=0, column="ID",
                                 value=df_record_storage["odkMetaInstanceID"])
        df_record_storage.to_csv(self.dir_openva + "/record_storage.csv",
                                 index=False)

        cols_keep = ["sid", "cause34", "metadataCode"]
        df_temp = merge(left=df_results[cols_keep],
                        left_on="sid",
                        right=df_data,
                        right_on="Generalmodule-sid",
                        how="right")
        df_temp.dropna(subset=["cause34"])
        df_temp.drop(columns="sid", inplace=True)
        df_temp.rename(columns={"meta-instanceID": "odkMetaInstanceID"},
                       inplace=True)
        df_temp["ID"] = df_temp["odkMetaInstanceID"]
        df_eva = df_temp.melt(id_vars=["ID"],
                              var_name="Attribute",
                              value_name="Value")
        df_eva.sort_values(by=["ID"], inplace=True)
        df_eva.to_csv(self.dir_openva + "/entity_attribute_value.csv",
                      index=False)

    def get_cod(self):
        """Create and execute R script to assign a COD with openVA; or call
           the SmartVA CLI to assign COD."""

        if self.pipeline_args.algorithm in ["InSilicoVA", "InterVA"]:
            r_script_in = os.path.join(self.dir_openva, self.run_date,
                                       "r_script_" + self.run_date + ".R")
            r_script_out = os.path.join(self.dir_openva, self.run_date,
                                        "r_script_" + self.run_date + ".Rout")
            r_args = ["R", "CMD", "BATCH", "--vanilla",
                      r_script_in, r_script_out]
            try:
                # capture_output=True not available in Python 3.6
                completed = subprocess.run(args=r_args,
                                           stdin=subprocess.PIPE,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE,
                                           check=True)
                self.successful_run = True
                return completed
            except subprocess.CalledProcessError as exc:
                if exc.returncode == 1:
                    self.successful_run = False
                    raise OpenVAError("Error running R script:" +
                                      str(exc.stderr)) from exc
        # if not ["InSilicoVA", "InterVA"], then run SmartVA
        try:
            os.makedirs(
                os.path.join(self.dir_openva, self.run_date)
            )
        except (PermissionError, OSError) as exc:
            raise OpenVAError("Unable to create openVA dir" +
                              str(exc)) from exc

        in_file = os.path.join(self.dir_openva, "openva_input.csv")
        out_dir = os.path.join(self.dir_openva, self.run_date)
        sva_args = [self.cli_smartva,
                    "--country", "{}".format(self.va_args.smartva_country),
                    "--hiv", "{}".format(self.va_args.smartva_hiv),
                    "--malaria", "{}".format(self.va_args.smartva_malaria),
                    "--hce", "{}".format(self.va_args.smartva_hce),
                    "--freetext", "{}".format(self.va_args.smartva_freetext),
                    "--figures", "{}".format(self.va_args.smartva_figures),
                    "--language", "{}".format(self.va_args.smartva_language),
                    in_file,
                    out_dir]
        try:
            # capture_output=True not available in Python 3.6
            completed = subprocess.run(args=sva_args,
                                       stdin=subprocess.PIPE,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       check=True)
            self.smartva_to_csv()
            self.successful_run = True
            return completed
        except subprocess.CalledProcessError as exc:
            if exc.returncode == 2:
                self.successful_run = False
                raise SmartVAError("Error running SmartVA:" +
                                   str(exc.stderr)) from exc
            if "Country list" in exc.stdout:
                self.successful_run = False
                raise SmartVAError("Problem with SmartVA " +
                                   "country code") from exc