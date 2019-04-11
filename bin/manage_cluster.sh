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
# Manage the BBoxDB cluster
#
#########################################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Load all required functions and variables
. $BBOXDB_HOME/bin/bootstrap.sh

# Nodes
bboxdb_nodes=$(read_nodes_file $bboxdb_node_file)

if [ -z "$bboxdb_nodes" ]; then
   echo "Your BBoxDB nodes ($bboxdb_node_file) are empty, please check your configuration" 
   exit -1
fi

zookeeper_nodes=$(read_nodes_file $zookeeper_node_file)
if [ -z "$zookeeper_nodes" ]; then
   echo "Your zookeeper nodes ($zookeeper_node_file) are empty, please check your configuration" 
   exit -1
fi

# Overwrite nodes by cli argument
if [ $# -eq 2 ]; then
   bboxdb_nodes=$2
   zookeeper_nodes=$2
fi

###
# Start the bboxdb
###
bboxdb_start() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh bboxdb_start" "Starting BBoxDB" "$bboxdb_nodes" $max_pending
}

###
# Start the bboxdb in debug mode
###
bboxdb_start_debug() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh bboxdb_start_debug" "Starting BBoxDB in debug mode" "$bboxdb_nodes" $max_pending
}

###
# Start the bboxdb in trace mode
###
bboxdb_start_trace() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh bboxdb_start_trace" "Starting BBoxDB in trace mode" "$bboxdb_nodes" $max_pending
}

###
# Start the bboxdb in remote debug mode
###
bboxdb_start_remote_debug() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh bboxdb_start_remote_debug" "Starting BBoxDB in remote debug mode" "$bboxdb_nodes" $max_pending
}

###
# Stop the bboxdb
###
bboxdb_stop() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh bboxdb_stop" "Stopping BBoxDB" "$bboxdb_nodes" $max_pending
}

###
# Update the bboxdb
###
bboxdb_update() {
   local_ip=$(get_local_ip)
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh bboxdb_update" "Update BBoxDB" "$bboxdb_nodes" $max_pending
   execute_parallel "rsync -l -H -p -o -g -D -t -r -v --numeric-ids -e 'ssh' $local_ip:$BBOXDB_HOME/bin/third_party_libs/ \$BBOXDB_HOME/bin/third_party_libs/" "Syncing custom libs" "$bboxdb_nodes" $max_pending
}

###
# Start zookeeper
###
zookeeper_start() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh zookeeper_start > /dev/null" "Starting Zookeeper" "$zookeeper_nodes" $max_pending
}

###
# Stop zookeeper
###
zookeeper_stop() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh zookeeper_stop > /dev/null" "Stopping Zookeeper" "$zookeeper_nodes" $max_pending
}

###
# Drop zookeeper
###
zookeeper_drop() {
   execute_parallel "\$BBOXDB_HOME/bin/manage_instance.sh zookeeper_drop > /dev/null" "Dropping Zookeeper database" "$zookeeper_nodes" $max_pending
}

case "$1" in  

bboxdb_start)
   bboxdb_start
   ;;  
bboxdb_start_debug)
   bboxdb_start_debug
   ;;  
bboxdb_start_trace)
   bboxdb_start_trace
   ;; 
bboxdb_start_remote_debug)
   bboxdb_start_remote_debug
   ;; 
bboxdb_stop)
   bboxdb_stop
   ;;  
bboxdb_upgrade)
   bboxdb_update
   ;;  
bboxdb_update)
   bboxdb_update
   ;;  
zookeeper_start)
   zookeeper_start
   ;;
zookeeper_stop)
   zookeeper_stop
   ;;
zookeeper_drop)
   zookeeper_drop
   ;;
*)
   echo "Usage: $0 {bboxdb_start | bboxdb_start_debug | bboxdb_start_trace | bboxdb_stop | bboxdb_update | zookeeper_start | zookeeper_stop | zookeeper_drop}"
   ;;  
esac

exit 0
