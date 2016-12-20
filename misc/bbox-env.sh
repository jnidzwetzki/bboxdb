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

################################
#JVM options
################################
jvm_ops=""

# Enable assertions
jvm_ops="$jvm_ops -ea"

# Enable JMX
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.port=$jmx_port"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.rmi.port=$jmx_port"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.ssl=false"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.authenticate=true"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.access.file=$BBOXDB_HOME/conf/jmxremote.access"
jvm_ops="$jvm_ops -Dcom.sun.management.jmxremote.password.file=$BBOXDB_HOME/conf/jmxremote.password"
