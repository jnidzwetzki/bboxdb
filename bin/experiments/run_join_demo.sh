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
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Load all required functions and variables
source $BBOXDB_HOME/bin/bootstrap.sh

base="/export/homes/nidzwetzki/germany_complete"
#base="/export/homes/nidzwetzki/osm-germany/berlin/osm"
wood="$base/WOOD"
road="$base/ROAD"

if [ ! -f ${wood}_FIXED ]; then
    egrep -v '"type":"Point"' ${wood} > ${wood}_FIXED
fi

if [ ! -f ${road}_FIXED ]; then
    egrep -v '"type":"Point"' ${road} > ${road}_FIXED
fi

$BBOXDB_HOME/bin/cli.sh -action delete_dgroup -dgroup osmgroup
$BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup osmgroup -replicationfactor 1 -dimensions 2 -maxregionsize 10485760
$BBOXDB_HOME/bin/cli.sh -action prepartition -file $road -format geojson -dgroup osmgroup -partitions 10
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_road
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_forest
$BBOXDB_HOME/bin/cli.sh -action import -file ${road}_FIXED -format geojson -table osmgroup_road
$BBOXDB_HOME/bin/cli.sh -action import -file ${wood}_FIXED -format geojson -table osmgroup_forest

# The query range
query_range="52.4,52.6:13.3,13.6"

echo "===== Range query on road ====="
read -p "Press enter to continue"
$BBOXDB_HOME/bin/cli.sh -action query_range -table osmgroup_road -bbox $query_range

echo "===== Range query on forest ====="
read -p "Press enter to continue"
$BBOXDB_HOME/bin/cli.sh -action query_range -table osmgroup_forest -bbox $query_range

echo "===== Bounding box join ====="
read -p "Press enter to continue"
$BBOXDB_HOME/bin/cli.sh -action query_join -table osmgroup_road:osmgroup_forest -bbox $query_range

echo "===== Spatial join ====="
read -p "Press enter to continue"
$BBOXDB_HOME/bin/cli.sh -action query_join -table osmgroup_road:osmgroup_forest -bbox $query_range -filter org.bboxdb.network.query.filter.UserDefinedGeoJsonSpatialFilter 
