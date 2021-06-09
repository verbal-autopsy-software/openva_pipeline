#!/bin/bash

# Pipeline.db
rm Pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" Pipeline.db -init pipelineDB.sql .exit

# copy_Pipeline.db
rm copy_Pipeline.db 
sqlcipher -cmd "pragma key = 'enilepiP'" copy_Pipeline.db -init copy_pipelineDB.sql .exit

# run_pipeline.db
rm run_Pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" run_Pipeline.db -init run_pipelineDB.sql .exit
