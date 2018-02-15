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
# Delete the whole cluster 
#
#########################################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Load all required functions and variables
source $BBOXDB_HOME/bin/bootstrap.sh


$BBOXDB_HOME/bin/manage_cluster.sh  bboxdb_stop
$BBOXDB_HOME/bin/manage_cluster.sh  bboxdb_update
$BBOXDB_HOME/bin/manage_cluster.sh  zookeeper_stop

# Nodes
bboxdb_nodes=$(read_nodes_file $bboxdb_node_file)

if [ -z "$bboxdb_nodes" ]; then
   echo "Your BBoxDB nodes ($bboxdb_node_file) are empty, please check your configuration" 
   exit -1
fi

for node in $bboxdb_nodes; do
   ssh $node "rm -r /tmp/bboxdb"; 
   ssh $node "mkdir -p /tmp/bboxdb/data"; 
done

$BBOXDB_HOME/bin/manage_cluster.sh  zookeeper_drop
$BBOXDB_HOME/bin/manage_cluster.sh  zookeeper_start
sleep 5


$BBOXDB_HOME/bin/manage_cluster.sh  bboxdb_start
sleep 5

if [[ $1 != "nopopulate" ]]; then
   $BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup testgroup -replicationfactor 1 -dimensions 2
   $BBOXDB_HOME/bin/cli.sh -action create_table -table testgroup_table1
   $BBOXDB_HOME/bin/cli.sh -action create_table -table testgroup_table2

   # Build table 1
   $BBOXDB_HOME/bin/cli.sh -action insert -table testgroup_table1 -key key1a -bbox 1:3,1:3 -value value1
   $BBOXDB_HOME/bin/cli.sh -action insert -table testgroup_table1 -key key2a -bbox 5:8,5:8 -value value2
   $BBOXDB_HOME/bin/cli.sh -action query -table testgroup_table1 -bbox 0:20:0:20

   # Build table 2
   $BBOXDB_HOME/bin/cli.sh -action insert -table testgroup_table2 -key key1b -bbox 1:2,1:2 -value value1
   $BBOXDB_HOME/bin/cli.sh -action insert -table testgroup_table2 -key key2b -bbox 1:10,1:10 -value value2
   $BBOXDB_HOME/bin/cli.sh -action query -table testgroup_table2 -bbox 0:20:0:20

   # Join
   $BBOXDB_HOME/bin/cli.sh -action join -table testgroup_table1:testgroup_table2 -bbox 0:20:0:20

   # Key query and delete
   $BBOXDB_HOME/bin/cli.sh -action query -table testgroup_table2 -key key1b
   $BBOXDB_HOME/bin/cli.sh -action delete -table testgroup_table2 -key key1b
   $BBOXDB_HOME/bin/cli.sh -action query -table testgroup_table2 -key key1b
   $BBOXDB_HOME/bin/cli.sh -action query -table testgroup_table2 -bbox 0:20:0:20
fi

