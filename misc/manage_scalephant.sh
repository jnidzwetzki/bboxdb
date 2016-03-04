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
# Start Zookeeper
###
start_zookeeper() {
    echo "Start Zookeeper"
    nohup java -cp $classpath -Dzookeeper.log.dir="$basedir/logs" org.apache.zookeeper.server.quorum.QuorumPeerMain $basedir/zoo.cfg > $basedir/zookeeper.log 2>&1 < /dev/null &
    if [ $? -eq 0 ]; then
       # Dump PID into file
       echo -n $! > $basedir/zookeeper.pid
    else
       echo "Unable to start zookeeper, check the logfiles for further information"
    fi
}

###
# Stop Zookeeper
###
stop_zookeeper() {
    if [ ! -f $basedir/zookeeper.pid ]; then
       echo "Unable to locate PID file"
    else
       echo "Stop Zookeeper"
       kill -9 $(cat $basedir/zookeeper.pid)
       rm $basedir/zookeeper.pid
    fi
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
start_zookeeper)
   start_zookeeper
   ;;
stop_zookeeper)
   stop_zookeeper
   ;;
*)
   echo "Usage: $0 {start|stop|update|start_zookeeper|stop_zookeeper}"
   ;;  
esac

exit 0
