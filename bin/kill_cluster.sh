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

$BBOXDB_HOME/bin/manage_cluster.sh zookeeper_drop
$BBOXDB_HOME/bin/manage_cluster.sh zookeeper_start
sleep 5

$BBOXDB_HOME/bin/manage_cluster.sh bboxdb_start
sleep 5

if [[ $1 != "nopopulate" ]]; then
   $BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup mydgroup -replicationfactor 2 -dimensions 2
   $BBOXDB_HOME/bin/cli.sh -action create_table -table mydgroup_table1
   $BBOXDB_HOME/bin/cli.sh -action create_table -table mydgroup_table2

   # Tuples of table1
   $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table1 -key tuple_a -bbox 1:8,1:2 -value value_a
   $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table1 -key tuple_b -bbox 4:6,0:5 -value value_b
   $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table1 -key tuple_c -bbox 8:10,8:10 -value value_c

   # Tuples of table2
   $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table2 -key tuple_1 -bbox 1:3,5:6 -value value_1
   $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table2 -key tuple_2 -bbox 4.5:5.5,1.5:4.5 -value value_2
   $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table2 -key tuple_3 -bbox 7.5:10,1.5:2.5 -value value_3

   # Join 
   $BBOXDB_HOME/bin/cli.sh -action join -table mydgroup_table1:mydgroup_table2 -bbox 0:10:0:8

   # Key query and delete
   $BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_table1 -key tuple_a
   $BBOXDB_HOME/bin/cli.sh -action delete -table mydgroup_table1 -key tuple_a
   $BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_table1 -key tuple_a
   $BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_table1 -bbox 0:20:0:20
fi

