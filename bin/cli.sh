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

# Logging
debug_args=""

if [ ! -z "$BBOXDB_LOG" ]; then
   if [ "$BBOXDB_LOG" == "debug" ]; then
        echo "Debug startup......"
        debug_args+="-Dlog4j.configuration=log4j_debug.properties"
   fi
   
   if [ "$BBOXDB_LOG" == "trace" ]; then
        echo "Trace startup......"
        debug_args+="-Dlog4j.configuration=log4j_trace.properties"
   fi
else
   debug_args+="-Dlog4j.configuration=log4j_warn.properties"
fi

java $jvm_ops_tools $debug_args -cp $classpath org.bboxdb.tools.cli.CLI "$@"

exit 0
