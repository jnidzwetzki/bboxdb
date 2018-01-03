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
# BBoxDB Pid
bboxdb_pid=$BBOXDB_HOME/misc/bboxdb.pid

# Zookeeper
zookeeper_workdir=$BBOXDB_HOME/misc/zookeeper
zookeeper_pid=$BBOXDB_HOME/misc/zookeeper.pid
zookeeper_clientport="2181"

# Node files
bboxdb_node_file=$BBOXDB_HOME/conf/bboxdb-nodes
zookeeper_node_file=$BBOXDB_HOME/conf/zookeeper-nodes

# Log dir
logdir=$BBOXDB_HOME/logs

# Port for JMX connetions
jmx_port=10999

# Default password is 'bboxdbMonitor' - !!change it!!!
jmx_password="bboxdbMonitor"

echo "monitorRoleUser  $jmx_password" > $BBOXDB_HOME/conf/jmxremote.password
echo "controlRoleUser  $jmx_password" >> $BBOXDB_HOME/conf/jmxremote.password
chmod 600 $BBOXDB_HOME/conf/jmxremote.password

################################
#JVM options for BBoxDB
################################
jvm_ops=""

# Enable assertions
jvm_ops="$jvm_ops -ea"

# Use the server mode
jvm_ops="$jvm_ops -server"

# Enable JMX
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.port=$jmx_port"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.rmi.port=$jmx_port"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.ssl=false"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.authenticate=true"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.access.file=$BBOXDB_HOME/conf/jmxremote.access"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.password.file=$BBOXDB_HOME/conf/jmxremote.password"

# Port for the remote debuger
jvm_debug_port=40010

################################
#JVM options for Tools
################################
jvm_ops_tools="-server -Xmx6096m"

