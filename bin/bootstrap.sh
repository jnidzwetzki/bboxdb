#!/bin/bash
#
# Bootstrap file for BBoxDB shell scripts
#
#
#########################################

# Include functions
source $BBOXDB_HOME/bin/bboxdb-env.sh
source $BBOXDB_HOME/bin/functions.sh

# Change working dir
cd $BBOXDB_HOME 

# Find all required jars
if [ -d $BBOXDB_HOME/target ]; then
   libs=$(find $BBOXDB_HOME/target/lib -name '*.jar' | xargs echo | tr ' ' ':')
   jar=$(find $BBOXDB_HOME/target -name 'bboxdb*.jar' | tail -1)
fi 

# Build classpath
classpath="$BBOXDB_HOME/conf:$libs:$jar"

