#!/bin/bash

# Pipeline.db
#rm Pipeline.db
#sqlcipher -cmd "pragma key = 'enilepiP'" Pipeline.db -init pipelineDB.sql .exit

# copy_Pipeline.db
rm copy_Pipeline.db 
sqlcipher -cmd "pragma key = 'enilepiP'" copy_Pipeline.db -init copy_pipelineDB.sql .exit

# run_pipeline.db
rm run_Pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" run_Pipeline.db -init run_pipelineDB.sql .exit

# wrongFields_pipeline.db
rm wrongFields_pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" wrongFields_pipeline.db -init wrongFields_pipelineDB.sql .exit

# wrongTables_pipeline.db
rm wrongTables_pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" wrongTables_pipeline.db -init wrongTables_pipelineDB.sql .exit
