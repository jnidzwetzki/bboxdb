#!/bin/bash
#
# Manage the Scalephant cluster
#
#########################################

# Scriptname and Path 
pushd `dirname $0` > /dev/null
basedir=`pwd`
fullpath=$(basename $0)
popd > /dev/null

##
# variables
##
done=" \x1b[33;32m[ Done ]\x1b[39;49;00m"
failed=" \x1b[31;31m[ Failed ]\x1b[39;49;00m"

# Max pending tasks
max_pending=3

# Execute command parallel on multiple nodes
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
         echo -n "Waiting for pending commands to finish..."  
         wait
         pending=0
         echo -e " $done"
       fi
   done

   if [ $pending -gt 0 ]; then
      echo -n "Waiting for pending commands to finish..."  
      wait
      echo -e " $done"
   fi
}

# Nodes
scalephant_nodes="${SCALEPHANT_SN}"
zookeeper_nodes="${SCALEPHANT_ZN}"

if [ -z "$scalephant_nodes" ]; then
   echo "Your environment variable \$(SCALEPHANT_SN) is empty. Please check your .scalephantrc"
   exit -1
fi

if [ -z "$zookeeper_nodes" ]; then
   echo "Your environment variable \$(SCALEPHANT_ZN) is empty. Please check your .scalephantrc"
   exit -1
fi

# Overwrite nodes by cli argument
if [ $# -eq 2 ]; then
   scalephant_nodes=$2
   zookeeper_nodes=$2
fi

###
# Start the scalephant
###
scalephant_start() {
   execute_parallel "cd $basedir; ./manage_instance.sh scalephant_start" "Starting Scalephant" "$scalephant_nodes" $max_pending
}

###
# Stop the scalephant
###
scalephant_stop() {
   execute_parallel "cd $basedir; ./manage_instance.sh scalephant_stop" "Stopping Scalephant" "$scalephant_nodes" $max_pending
}

###
# Update the scalephant
###
scalephant_update() {
   execute_parallel "cd $basedir; ./manage_instance.sh scalephant_update" "Update Scalephant" "$scalephant_nodes" $max_pending
}

###
# Start zookeeper
###
zookeeper_start() {
   execute_parallel "cd $basedir; ./manage_instance.sh zookeeper_start > /dev/null" "Starting Zookeeper" "$zookeeper_nodes" $max_pending
}

###
# Stop zookeeper
###
zookeeper_stop() {
   execute_parallel "cd $basedir; ./manage_instance.sh zookeeper_stop > /dev/null" "Stopping Zookeeper" "$zookeeper_nodes" $max_pending
}

case "$1" in  

scalephant_start)
   scalephant_start
   ;;  
scalephant_stop)
   scalephant_stop
   ;;  
scalephant_update)
   scalephant_update
   ;;  
zookeeper_start)
   zookeeper_start
   ;;
zookeeper_stop)
   zookeeper_stop
   ;;
*)
   echo "Usage: $0 {scalephant_start|scalephant_stop|scalephant_update|zookeeper_start|zookeeper_stop}"
   ;;  
esac

exit 0
