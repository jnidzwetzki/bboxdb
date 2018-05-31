#!/bin/sh
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
# Entry point for Docker
######################

if [ -z ${ZK_HOSTS+x} ]; then
   echo "Error: Environment variable ZK_HOSTS is not set"
   exit
fi

zk_file="/bboxdb/conf/zookeeper-nodes"

rm $zk_file

echo $ZK_HOSTS | sed s/[[:blank:]]//g | sed "s/,/ /g" | while read zookeeper; do 
   echo $zookeeper >> $zk_file; 
done

 /bboxdb/bin/manage_instance.sh bboxdb_start
