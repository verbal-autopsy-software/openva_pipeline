-- Used for testing pipeline (when DB has missing tables)

-- Pipeline Configuration
CREATE TABLE A
(
  algorithmMetadataCode char(100), -- see table Algorithm_Metadata_Options (below)
  codSource             char ( 6) NOT NULL CHECK (codSource IN ("ICD10", "WHO", "Tariff")),
  algorithm             char(  8) NOT NULL CHECK (algorithm IN ("InterVA", "Insilico", "SmartVA")),
  workingDirectory      char(100)
);

INSERT INTO A
  (algorithmMetadataCode, codSource, algorithm, workingDirectory)
  VALUES("SmartVA|2.0.0_a8|PHMRCShort|1|PHMRCShort|1", "Tariff", "SmartVA", ".");

.exit
