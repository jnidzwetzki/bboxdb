#!/bin/bash
#
# Start the GUI of the BBoxDB 
#
#
#########################################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

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

echo "Start the GUI...."

java $jvm_ops_tools -cp $classpath org.bboxdb.tools.gui.Main

exit 0
