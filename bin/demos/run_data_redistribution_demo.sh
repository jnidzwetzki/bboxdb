#!/bin/bash
#*******************************************************************************
#
#    Copyright (C) 2015-2021 the BBoxDB project
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

if [[ $# -ne 1 ]]; then
    echo "Illegal number of parameters. Demomode exprected" >&2
    exit 2
fi

# Load all required functions and variables
source $BBOXDB_HOME/bin/bootstrap.sh

demomode=$1

if [[ $demomode == "treedemo" ]]; then
   # Testfiles
   #datafiles="/export/homes/nidzwetzki/osm-germany/nordrhein-westfalen/osm/TREE:/export/homes/nidzwetzki/osm-germany/berlin/osm/TREE:/export/homes/nidzwetzki/osm-germany/hamburg/osm/TREE:/export/homes/nidzwetzki/osm-germany/hessen/osm/TREE"
   datafiles=$(find /BIG/nidzwetzki/datasets/osm/german-states -name 'TREE' | xargs echo | tr ' ' ':')
   $BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.demo.DataRedistributionLoader $datafiles 6 3 16 32 newton1:50181 mycluster
fi

if [[ $demomode == "tree" ]]; then
   # Testfiles
   #datafiles="/export/homes/nidzwetzki/osm-germany/nordrhein-westfalen/osm/TREE:/export/homes/nidzwetzki/osm-germany/berlin/osm/TREE:/export/homes/nidzwetzki/osm-germany/hamburg/osm/TREE:/export/homes/nidzwetzki/osm-germany/hessen/osm/TREE"
   datafiles=$(find /BIG/nidzwetzki/datasets/osm/german-states -name 'TREE' | xargs echo | tr ' ' ':')
   $BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.demo.DataRedistributionLoader $datafiles 16 6 32 64 newton1:50181 mycluster
fi

if [[ $demomode == "treeauto" ]]; then
   datafiles=$(find /BIG/nidzwetzki/datasets/osm/german-states -name 'TREE' | xargs echo | tr ' ' ':')
   $BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.demo.DataRedistributionLoader $datafiles 16 6 16 32 newton1:50181 mycluster true
fi

if [[ $demomode == "road" ]]; then
   datafiles=$(find /BIG/nidzwetzki/datasets/osm/german-states -name 'ROAD' | xargs echo | tr ' ' ':')
   $BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.demo.DataRedistributionLoader $datafiles 16 6 768 1024 newton1:50181 mycluster
fi





