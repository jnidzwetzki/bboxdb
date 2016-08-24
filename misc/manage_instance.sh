#!/bin/bash
#
# Manage the local scalephant instance
#
#
#########################################

jsvc_file="commons-daemon-1.0.15-native-src.tar.gz"
jsvc_url="http://www.apache.org/dist/commons/daemon/source/$jsvc_file"

# Scriptname and Path 
pushd `dirname $0` > /dev/null
basedir=`pwd`
fullpath=$(basename $0)
popd > /dev/null

# Find all jars
cd $basedir

# Does a build exists? 
if [ -d ../target ]; then
   libs=$(find ../target/lib -name '*.jar' | xargs echo | tr ' ' ':')
   jar=$(ls -1 ../target/scalephant*.jar | tail -1)
fi 

classpath="$basedir/../conf:$libs:$jar"

# Zookeeper
zookeeper_workdir=$basedir/zookeeper
zookeeper_clientport="2181"

# Nodes
zookeeper_nodes="${SCALEPHANT_ZN}"

if [ -z "$zookeeper_nodes" ]; then
   echo "Your environment variable \$(SCALEPHANT_ZN) is empty. Please check your .scalephantrc"
   exit -1
fi

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
          exit 2
       fi

       tar zxvf $jsvc_file > /dev/null
       cd commons-daemon-1.0.15-native-src/unix 
       ./configure
       make

       if [ ! -x jsvc ]; then
           echo "Error: unable to compile jsvc, exiting"
           exit 2
       fi

       mv jsvc $basedir
       cd $basedir
   fi
}

###
# Update and build the scalephant
###
scalephant_update() {
   echo "Upgrade the scalephant"
   cd ..
   git pull
   mvn package -DskipTests
}

###
# Start the scalephant
###
scalephant_start() {
    echo "Start the scalephant"
    download_jsvc
    cd $basedir

    if [ ! -d logs/ ]; then
        mkdir logs
    fi

    # Activate JVM start debugging
    debug_flag=""
    #debug_flag="-debug"
 
    config="$basedir/../conf/scalephant.yaml"

    if [ ! -f $config ]; then
        echo "Unable to locate scalepahnt config $config"
        exit -1
    fi

    # Configure zookeeper contact points
    zookeeper_connect="" 
    for i in $zookeeper_nodes; do
        if [[ -z "$zookeeper_connect" ]]; then
            zookeeper_connect="['$i:$zookeeper_clientport'"
        else
            zookeeper_connect="$zookeeper_connect, '$i:$zookeeper_clientport'"
        fi
    done
    zookeeper_connect="$zookeeper_connect]"

    sed -i "s/zookeepernodes: .*/zookeepernodes: $zookeeper_connect/" $config

    ./jsvc $debug_flag -outfile $basedir/logs/scalephant.log -pidfile $basedir/scalephant.pid -cwd $basedir -cp $classpath de.fernunihagen.dna.scalephant.ScalephantMain
}

###
# Stop the scalephant
###
scalephant_stop() {
    echo "Stop the scalephant"
    cd $basedir
    ./jsvc -pidfile $basedir/scalephant.pid -stop -cwd $basedir -cp $classpath de.fernunihagen.dna.scalephant.ScalephantMain

    # Was stop successfully?
    if [ -f $basedir/scalephant.pid ]; then
        pid=$(cat $basedir/scalephant.pid)
        if [ -d /proc/$pid ]; then
            echo "Normal shutdown was not successfully, killing process...."
            kill -9 $pid
        fi
    fi
}

###
# Zookeeper start
###
zookeeper_start() {
    # Check for running instance
    if [ -f $basedir/zookeeper.pid ]; then
       echo "Found old zookeeper pid, check process list or remove pid"
       exit 2
    fi

    echo "Start Zookeeper"

    if [ ! -d logs/ ]; then
        mkdir logs
    fi

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
autopurge.purgeInterval=2
EOF

    serverid=0
    instanceid=-1
    hostname=$(hostname)
    localip=$(hostname --ip-address)

    for i in $zookeeper_nodes; do
       echo "server.$serverid=$i:2888:3888" >> $basedir/zoo.cfg

       # Store the id of this node
       if [ "$hostname" == "$i" ] || [ "$localip" == "$i" ]; then
          instanceid=$serverid
       fi

       serverid=$((serverid+1))
    done

    # Write the id of this instance
    echo $instanceid > $zookeeper_workdir/myid
 
    # Start zookeeper
    nohup java -cp $classpath -Dzookeeper.log.dir="$basedir/logs" org.apache.zookeeper.server.quorum.QuorumPeerMain $basedir/zoo.cfg > $basedir/logs/zookeeper.log 2>&1 < /dev/null &
    
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

###
# Zookeeper drop - Drop the zookeeper database
###
zookeeper_drop() {
   
   zookeeper_stop
  
   echo "Drop zookeeper database" 
  
   # Drop zookeeper work dir
   if [ -d $zookeeper_workdir ]; then
      if [ ! -f $zookeeper_workdir/myid ]; then 
           echo "File $zookeeper_workdir/myid not found, skipping delete. Maybe $zookeeper_workdir is not a zookeeper workdir"
           exit -1
      fi

      rm -r $zookeeper_workdir
   fi
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
zookeeper_client)
   zookeeper_client
   ;;
zookeeper_drop)
   zookeeper_drop
   ;;
*)
   echo "Usage: $0 {scalephant_start | scalephant_stop | scalephant_update | zookeeper_start | zookeeper_stop | zookeeper_client | zookeeper_drop}"
   ;;  
esac

exit 0
