#!/bin/bash
#
# Start the CLI of the BBoxDB 
#
#
#########################################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Load all required functions and variables
source $BBOXDB_HOME/bin/bootstrap.sh

# Check parameter
if [ "$#" -eq 0 ]; then
    echo "Usage: $0 <classname> <parameter>"
    exit -1
fi

java $jvm_ops_tools $jvm_ops_tools -cp $classpath "$@"

exit 0
