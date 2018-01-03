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

