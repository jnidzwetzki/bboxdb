#!/bin/bash
#*******************************************************************************
#
#    Copyright (C) 2015-2018 the BBoxDB project
#  
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#  
#      http://www.apache.org/licenses/LICENSE-2.0
#  
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License. 
#    
#*******************************************************************************
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
