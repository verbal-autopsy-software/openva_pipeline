date() 
library(openVA); library(CrossVA) 
getwd() 
records <- read.csv('tests/OpenVAFiles/openVA_input.csv') 
data <- map_records_insilicova(records) 
names(data) <- tolower(names(data)) 
odkMetaInstanceID <- as.character(records$meta.instanceID) 
data$id <- odkMetaInstanceID 
results <- insilico(data = data, 
	 data.type = 'WHO2012', 
	 isNumeric = FALSE, 
	 updateCondProb = TRUE, 
	 keepProbbase.level = TRUE, 
	 CondProb = NULL, 
	 CondProbNum = NULL, 
	 datacheck = TRUE, 
	 datacheck.missing = TRUE, 
	 warning.write = TRUE, 
	 directory = 'tests/OpenVAFiles/2018-10-24_17:32:45', 
	 external.sep = TRUE, 
	 Nsim = 4000, 
	 thin = 10, 
	 burnin = 2000, 
	 auto.length = TRUE, 
	 conv.csmf = 0.02, 
	 jump.scale = 0.1, 
	 levels.prior = NULL, 
	 levels.strength = 1, 
	 trunc.min = 0.0001, 
	 trunc.max = 0.9999, 
	 subpop = NULL, 
	 java_option = '-Xmx1g', 
	 seed = 1, 
	 phy.code = NULL, 
	 phy.cat = NULL, 
	 phy.unknown = NULL, 
	 phy.external = NULL, 
	 phy.debias = NULL, 
	 exclude.impossible.cause = 'subset', 
	 no.is.missing = FALSE, 
	 indiv.CI = NULL, 
	 groupcode = FALSE) 
sex <- ifelse(tolower(data$male)=='y', 'Male', 'Female') 
cod <- getTopCOD(results) 
hasCOD <- as.character(data$id) %in% as.character(levels(cod$ID)) 
dob <- as.Date(as.character(records$consented.deceased_CRVS.info_on_deceased.Id10021), '%b %d, %Y') 
dod <- as.Date(as.character(records$consented.deceased_CRVS.info_on_deceased.Id10023), '%b %d, %Y') 
age <- floor(records$consented.deceased_CRVS.info_on_deceased.ageInDays/365.25) 
## create matrices for DHIS2 blob (data2) and transfer database (data3) 
## first column must be ID 
metadataCode <- 'InSilicoVA|1.1.4|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1'
cod2 <- rep('MISSING', nrow(data)); cod2[hasCOD] <- as.character(cod[,2]) 
data2 <- cbind(data[,-1], cod2, metadataCode) 
names(data2) <- c(names(data[,-1]), 'Cause of Death', 'Metadata') 
evaBlob <- cbind(rep(as.character(data[,1]), each=ncol(data2)), rep(names(data2)), c(apply(data2, 1, c))) 
colnames(evaBlob) <- c('ID', 'Attribute', 'Value') 
write.csv(evaBlob, file='tests/OpenVAFiles/entityAttributeValue.csv', row.names=FALSE, na='') 

data3 <- cbind(as.character(data[,1]), sex, dob, dod, age, cod2, metadataCode, odkMetaInstanceID, data[,-1]) 
names(data3) <- c('id', 'sex', 'dob', 'dod', 'age', 'cod', 'metadataCode', 'odkMetaInstanceID', names(data[,-1])) 
write.csv(data3, file='tests/OpenVAFiles/recordStorage.csv', row.names=FALSE, na='') 
