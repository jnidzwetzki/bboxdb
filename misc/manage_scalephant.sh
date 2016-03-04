#!/bin/sh
#
# Manage the scalephant
#
#
#########################################

jsvc_file="commons-daemon-1.0.15-native-src.tar.gz"
jsvc_url="http://www.apache.org/dist/commons/daemon/source/$jsvc_file"

# Change path to basedir
fullpath=$(readlink -f "$0")
basedir=$(dirname $fullpath)

# Find all jars
cd $basedir
libs=$(find ../target/lib -name '*.jar' | xargs echo | tr ' ' ':')
jar=$(ls -1 ../target/scalephant*.jar | tail -1)
classpath="$basedir/../conf:$libs:$jar"

# Zookeeper
zookeeper_workdir=$basedir/zookeeper
zookeeper_nodes="node1 node2 node3"
zookeeper_clientport="2181"

###
# Download and compile jsvc if not installed
###
download_jsvc() {
   cd $basedir
   if [ ! -x ./jsvc ]; then
       echo "JSVC not found, downloading"
       wget $jsvc_url
    
       if [ ! -f $jsvc_file ]; then
          echo "Error: unable to download commons-daemon, exiting"
          exit -1
       fi

       tar zxvf $jsvc_file > /dev/null
       cd commons-daemon-1.0.15-native-src/unix 
       ./configure
       make

       if [ ! -x jsvc ]; then
           echo "Error: unable to compile jsvc, exiting"
           exit -1
       fi

       mv jsvc $basedir
       cd $basedir
   fi
}

###
# Build the scalephant
###
update_and_build() {
   echo "Upgrade the scalephant"
   cd ..
   git pull
   mvn package -DskipTests
}

###
# Start the scalephant
###
start() {
    echo "Start the scalephant"
    download_jsvc
    cd $basedir

    if [ ! -d ../logs/ ]; then
        mkdir ../logs
    fi

    # Activate JVM start debugging
    debug_flag=""
    #debug_flag="-debug"

    ./jsvc $debug_flag -outfile $basedir/logs/scalephant.log -pidfile $basedir/scalephant.pid -cwd $basedir -cp $classpath de.fernunihagen.dna.jkn.scalephant.ScalephantMain
}

###
# Stop the scalephant
###
stop() {
    echo "Stop the scalephant"
    cd $basedir
    ./jsvc -pidfile $basedir/scalephant.pid -stop -cwd $basedir -cp $classpath de.fernunihagen.dna.jkn.scalephant.ScalephantMain
}

###
# Zookeeper start
###
zookeeper_start() {
    echo "Start Zookeeper"

    # Create work dir
    if [ ! -d $zookeeper_workdir ]; then
       mkdir -p $zookeeper_workdir
    fi
   
    # Write zookeeper config
cat << EOF > $basedir/zoo.cfg
tickTime=2000
dataDir=$zookeeper_workdir
clientPort=$zookeeper_clientport
initLimit=5
syncLimit=2
EOF

    for i in $zookeeper_nodes; do
       echo $i:2888:3888 >> $basedir/zoo.cfg
    done
 
    # Start zookeeper
    nohup java -cp $classpath -Dzookeeper.log.dir="$basedir/logs" org.apache.zookeeper.server.quorum.QuorumPeerMain $basedir/zoo.cfg > $basedir/zookeeper.log 2>&1 < /dev/null &
    
    if [ $? -eq 0 ]; then
       # Dump PID into file
       echo -n $! > $basedir/zookeeper.pid
    else
       echo "Unable to start zookeeper, check the logfiles for further information"
    fi
}

###
# Zookeeper stop
###
zookeeper_stop() {
    if [ ! -f $basedir/zookeeper.pid ]; then
       echo "Unable to locate PID file"
    else
       echo "Stop Zookeeper"
       kill -9 $(cat $basedir/zookeeper.pid)
       rm $basedir/zookeeper.pid
    fi
}

###
# Zookeeper client
###
zookeeper_client() {
    echo "Connecting to Zookeeper"
    java -cp $classpath org.apache.zookeeper.ZooKeeperMain -server 127.0.0.1:$zookeeper_clientport
}


case "$1" in  

start)
   start
   ;;  
stop)
   stop
   ;;  
update)
   update_and_build
   ;;  
zookeeper_start)
   zookeeper_start
   ;;
zookeeper_stop)
   zookeeper_stop
   ;;
zookeeper_client)
   zookeeper_client
   ;;
*)
   echo "Usage: $0 {start|stop|update|zookeeper_start|zookeeper_stop|zookeeper_client}"
   ;;  
esac

exit 0
