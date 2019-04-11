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
##
# variables
##

done=" \x1b[33;32m[ Done ]\x1b[39;49;00m"
failed=" \x1b[31;31m[ Failed ]\x1b[39;49;00m"

# Max pending tasks (for parallel operations)
max_pending=3

###
# Read a nodes file
### 
read_nodes_file() {
   
   if [ $# -ne 1 ]; then
       echo "Error not enough parameters passed to function call"
       exit -1
   fi

   file=$1

   if [ ! -f $file ]; then
      echo "Unable to open $file for reading, exiting"
      exit -1
   fi

   result=""

   # Read from FD 10, to prevent some SSH related issues
   while read -u10 line; do
      # Skip empty lines
      if [[ -z $line ]]; then
          continue
      fi

      # Skip lines with comment
      if [[ $line == \#* ]]; then
          continue
      fi 

      result="$result $line"
   done 10< $file

  echo $result 
}

###
# Execute command parallel on multiple nodes
###
execute_parallel() {

   if [ $# -ne 4 ]; then
       echo "Error not enough parameters passed to function call"
       exit -1
   fi

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

###
# Get the local ip
###
get_local_ip() {
   echo $(ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p' | head -1)
}


