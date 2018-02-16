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

datafiles=$(find /export/homes/nidzwetzki/osm-germany -name 'ROAD' | xargs echo | tr ' ' ':')

# Testfiles
#datafiles="/export/homes/nidzwetzki/osm-germany/berlin/osm/ROAD:/export/homes/nidzwetzki/osm-germany/hamburg/osm/ROAD"

$BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.demo.DataRedistributionLoader $datafiles node1:2181 mycluster
