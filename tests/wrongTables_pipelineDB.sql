-- Used for testing pipeline (when DB has missing tables)
PRAGMA key = "enilepiP";

-- Pipeline Configuration
CREATE TABLE A
(
  algorithmMetadataCode char(100), -- see table Algorithm_Metadata_Options (below)
  codSource             char ( 6) NOT NULL CHECK (codSource IN ("ICD10", "WHO", "Tariff")),
  algorithm             char(  8) NOT NULL CHECK (algorithm IN ("InterVA", "InSilicoVA", "SmartVA")),
  workingDirectory      char(100)
);

INSERT INTO A
  (algorithmMetadataCode, codSource, algorithm, workingDirectory)
  VALUES("InSilicoVA|1.1.4|InterVA|5|2016 WHO Verbal Autopsy Form|v1_4_1", "WHO", "InSilicoVA", ".");

.exit
