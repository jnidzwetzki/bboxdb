##
# variables
##

done=" \x1b[33;32m[ Done ]\x1b[39;49;00m"
failed=" \x1b[31;31m[ Failed ]\x1b[39;49;00m"

# Node files
bboxdb_node_file=$BBOXDB_HOME/conf/bboxdb-nodes
zookeeper_node_file=$BBOXDB_HOME/conf/zookeeper-nodes

# Max pending tasks (for parallel operations)
max_pending=3

###
# Read a nodes file
### 
read_nodes_file() {
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


