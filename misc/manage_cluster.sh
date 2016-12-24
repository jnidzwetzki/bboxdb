#!/bin/bash
#
# Manage the BBoxDB cluster
#
#########################################

# Home dir
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Include functions
source $BBOXDB_HOME/misc/bboxdb-env.sh
source $BBOXDB_HOME/misc/functions.sh

###
# Execute command parallel on multiple nodes
###
function execute_parallel() {
   command=$1
   task=$2
   nodes=$3
   max_pending=$4

   # Number of pending starts
   pending=0

   for node in $nodes; do
      echo "$task on Node $node "
      ssh $node "$command" &

      pending=$((pending + 1))

      if [ $pending -ge $max_pending ]; then
         echo "Waiting for pending commands to finish..."  
         wait
         pending=0
         echo -e "Pending commands complete $done"
       fi
   done

   if [ $pending -gt 0 ]; then
      echo "Waiting for pending commands to finish..."  
      wait
      echo -e "Pending commands complete $done"
   fi
}

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
   execute_parallel "\$BBOXDB_HOME/misc/manage_instance.sh bboxdb_start" "Starting BBoxDB" "$bboxdb_nodes" $max_pending
}

###
# Start the bboxdb in debug mode
###
bboxdb_start_debug() {
   execute_parallel "\$BBOXDB_HOME/misc/manage_instance.sh bboxdb_start_debug" "Starting BBoxDB" "$bboxdb_nodes" $max_pending
}

###
# Stop the bboxdb
###
bboxdb_stop() {
   execute_parallel "\$BBOXDB_HOME/misc/manage_instance.sh bboxdb_stop" "Stopping BBoxDB" "$bboxdb_nodes" $max_pending
}

###
# Update the bboxdb
###
bboxdb_update() {
   execute_parallel "\$BBOXDB_HOME/misc/manage_instance.sh bboxdb_update" "Update BBoxDB" "$bboxdb_nodes" $max_pending
}

###
# Start zookeeper
###
zookeeper_start() {
   execute_parallel "\$BBOXDB_HOME/misc/manage_instance.sh zookeeper_start > /dev/null" "Starting Zookeeper" "$zookeeper_nodes" $max_pending
}

###
# Stop zookeeper
###
zookeeper_stop() {
   execute_parallel "\$BBOXDB_HOME/misc/manage_instance.sh zookeeper_stop > /dev/null" "Stopping Zookeeper" "$zookeeper_nodes" $max_pending
}

###
# Drop zookeeper
###
zookeeper_drop() {
   execute_parallel "\$BBOXDB_HOME/misc/manage_instance.sh zookeeper_drop > /dev/null" "Dropping Zookeeper database" "$zookeeper_nodes" $max_pending
}

case "$1" in  

bboxdb_start)
   bboxdb_start
   ;;  
bboxdb_start_debug)
   bboxdb_start_debug
   ;;  
bboxdb_stop)
   bboxdb_stop
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
   echo "Usage: $0 {bboxdb_start | bboxdb_start_debug | bboxdb_stop | bboxdb_update | zookeeper_start | zookeeper_stop | zookeeper_drop}"
   ;;  
esac

exit 0
