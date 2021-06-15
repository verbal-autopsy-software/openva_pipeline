#!/bin/bash

# copy_Pipeline.db
[ -e copy_Pipeline.db ] && rm copy_Pipeline.db 
sqlcipher -cmd "pragma key = 'enilepiP'" copy_Pipeline.db -init copy_pipelineDB.sql .exit

# run_Pipeline.db
[ -e run_Pipeline.db ] && rm run_Pipeline.db 
sqlcipher -cmd "pragma key = 'enilepiP'" run_Pipeline.db -init run_pipelineDB.sql .exit

# wrongFields_Pipeline.db
[ -e wrongFields_Pipeline.db ] && rm wrongFields_Pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" wrongFields_Pipeline.db -init wrongFields_pipelineDB.sql .exit

# wrongTables_Pipeline.db
[ -e wrongTables_Pipeline.db ] && rm wrongTables_Pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" wrongTables_Pipeline.db -init wrongTables_pipelineDB.sql .exit

# InterVA_Pipeline.db
# InterVA_Pipeline.db
# sqlcipher -cmd "pragma key = 'enilepiP'" InterVA_Pipeline.db -init interva_pipelineDB.sql .exit

# copy_smartVA_Pipeline.db
[ -e copy_smartVA_Pipeline.db ] && rm copy_smartVA_Pipeline.db
sqlcipher -cmd "pragma key = 'enilepiP'" copy_smartVA_Pipeline.db -init copy_smartva_pipelineDB.sql .exit
