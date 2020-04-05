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
# Import data for the demo 
#
#########################################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your installation."
   exit -1
fi

# Load all required functions and variables
source $BBOXDB_HOME/bin/bootstrap.sh

make_script_on_error_fail

function wait_if_needed() {
	if [[ $1 != "nowait" ]]; then
	   read -p "Press enter to continue"
	fi
}

if [ "$#" -lt 1 ]; then
   echo "$0 <basedir> [nowait]"
   exit -1
fi

# The query range
query_range="52.4,52.6:13.3,13.6"

echo "===== Range query on road ====="
wait_if_needed $2
$BBOXDB_HOME/bin/cli.sh -action query_range -table ${groupname}_road -bbox $query_range

echo "===== Range query on forest ====="
wait_if_needed $2
$BBOXDB_HOME/bin/cli.sh -action query_range -table ${groupname}_forest -bbox $query_range

echo "===== Bounding box join ====="
wait_if_needed $2
$BBOXDB_HOME/bin/cli.sh -action query_join -table ${groupname}_road:${groupname}_forest -bbox $query_range

echo "===== Spatial join ====="
wait_if_needed $2
$BBOXDB_HOME/bin/cli.sh -action query_join -table ${groupname}_road:${groupname}_forest -bbox $query_range -filter org.bboxdb.network.query.filter.UserDefinedGeoJsonSpatialFilter 

exit_script_successfully

