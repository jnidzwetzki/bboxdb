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

#grep name /export/homes/nidzwetzki/osm-germany/berlin/osm/ROAD > /export/homes/nidzwetzki/osm-germany/berlin/osm/ROAD_NAME
#grep name /export/homes/nidzwetzki/osm-germany/berlin/osm/WOOD > /export/homes/nidzwetzki/osm-germany/berlin/osm/WOOD_NAME

$BBOXDB_HOME/bin/cli.sh -action delete_dgroup -dgroup osmgroup
$BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup osmgroup -replicationfactor 1 -dimensions 2
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_road
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_forrest
$BBOXDB_HOME/bin/cli.sh -action import -file /export/homes/nidzwetzki/osm-germany/berlin/osm/ROAD_NAME  -format geojson -table osmgroup_road
$BBOXDB_HOME/bin/cli.sh -action import -file /export/homes/nidzwetzki/osm-germany/berlin/osm/WOOD_NAME  -format geojson -table osmgroup_forrest


$BBOXDB_HOME/bin/cli.sh -action query -table osmgroup_road -bbox  52.520,52.525:13.410,13.415

$BBOXDB_HOME/bin/cli.sh -action query -table osmgroup_forrest -bbox  52.520,52.525:13.410,13.415

$BBOXDB_HOME/bin/cli.sh -action join -table osmgroup_road:osmgroup_forrest -bbox 52.4,52.6:13.3,13.6
