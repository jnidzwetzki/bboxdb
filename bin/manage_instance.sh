#!/bin/bash
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
# Manage the local BBoxDB instance
#
#
#########################################

# Is the environment configured?
if [ -z "$BBOXDB_HOME" ]; then
   echo "Your environment variable \$(BBOXDB_HOME) is empty. Please check your .bboxdbrc"
   exit -1
fi

# Load all required functions and variables
source $BBOXDB_HOME/bin/bootstrap.sh


# Discover zookeeper nodes
zookeeper_nodes=$(read_nodes_file $zookeeper_node_file)
if [ -z "$zookeeper_nodes" ]; then
   echo -e "Your Zookeeper nodes ($zookeeper_node_file) are empty, please check your configuration $failed" 
   exit -1
fi

###
# Update and build the bboxdb
###
bboxdb_update() {
   echo "Update the BBoxDB"
   
   git pull
   
   # Remove old jars
   if [ -d target/lib/ ]; then
      rm -r target/lib/
   fi
   
   rm -f target/*.jar
   
   mvn package -DskipTests -Dassembly.skipAssembly=true
   
   echo -e "BBoxDB data is successfully updated $done"
}

###
# Start the bboxdb
###
bboxdb_start() {
    echo "Starting the BBoxDB"
    cd $BBOXDB_HOME

    # Is already running?
    if [ -f $bboxdb_pid ]; then
        pid=$(cat $bboxdb_pid)
        if [ -d /proc/$pid ]; then
            echo "BBoxDB is already running PID ($pid)"
            return
        fi
    fi
    
    # Create log dir if needed
    if [ ! -d $logdir ]; then
        mkdir $logdir
    fi

    debug_args=""

    # Logging
    if [ ! -z "$BBOXDB_LOG" ]; then
	    if [ "$BBOXDB_LOG" == "debug" ]; then
	         echo "Debug startup......"
	         debug_args+="-Dlog4j.configuration=log4j_debug.properties"
	    fi
	    
	    if [ "$BBOXDB_LOG" == "trace" ]; then
	         echo "Trace startup......"
	         debug_args+="-Dlog4j.configuration=log4j_trace.properties"
	    fi
    fi

    # Remote debug
    if [ ! -z "$BBOXDB_REMOTE_DEBUG" ]; then
        echo "Enabling remote debuging on port $jvm_debug_port"
        debug_args+="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=$jvm_debug_port,suspend=n"
    fi
    
    config="$BBOXDB_HOME/conf/bboxdb.yaml"

    if [ ! -f $config ]; then
        echo -e "Unable to locate bboxdb config $config $failed"
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

    if [ -f $logdir/bboxdb.out.log ]; then
         rm $logdir/bboxdb.out.log
    fi

    cd $BBOXDB_HOME/misc
    
    # Start zookeeper
    nohup java $debug_args $jvm_ops -cp $classpath -Dbboxdb.log.dir="$logdir" org.bboxdb.BBoxDBMain > $logdir/bboxdb.out.log 2>&1 < /dev/null &
    
    if [ $? -eq 0 ]; then
       # Dump PID into file
       echo -n $! > $bboxdb_pid
    else
       echo -e "Unable to start BBoxDB, check the logfiles for further information $failed"
       exit -1
    fi
    
	echo -e "BBoxDB is successfully started $done"
}

###
# Stop the bboxdb
###
bboxdb_stop() {
    echo "Stopping the BBoxDB"
    cd $BBOXDB_HOME/misc
    
    if [ ! -f $bboxdb_pid ]; then
       echo -e "PID file $bboxdb_pid not found, BBoxDB seems to be already down $failed"
       exit -1
    else 
        pid=$(cat $bboxdb_pid)
        if [ ! -d /proc/$pid ]; then
          echo -e "PID file $bboxdb_pid found, but process ($pid) is already terminated $done"
          rm $bboxdb_pid
          exit -1
        fi
    fi
    
    # Call shutdown MBean
    java -cp $classpath org.bboxdb.Shutdown $jmx_port $jmx_password

    pid=$(cat $bboxdb_pid)

    # Wait up to 30 seconds for shutdown
    waits=0
    while [ -d /proc/$pid ]; do
       
       if [ $waits -gt 60 ]; then
          break; 
       fi
       
       sleep 1
       waits=$((waits + 1))
    done
    
    rm $bboxdb_pid
    
    if [ -d /proc/$pid ]; then
        echo "Normal shutdown was not successfully, killing process...."
        kill -9 $pid
    fi
    
    echo -e "BBoxDB is successfully stopped $done"
}

###
# Zookeeper start
###
zookeeper_start() {
    # Check for running instance
    if [ -f $zookeeper_pid ]; then
       echo -e "Found old zookeeper pid, check process list or remove pid $failed"
       exit 2
    fi

    echo "Starting Zookeeper"

    if [ ! -d $logdir ]; then
        mkdir $logdir
    fi

    # Create work dir
    if [ ! -d $zookeeper_workdir ]; then
       mkdir -p $zookeeper_workdir
    fi
   
    # Write zookeeper config
cat << EOF > $BBOXDB_HOME/misc/zookeeper.cfg
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
       echo "server.$serverid=$i:2888:3888" >> $BBOXDB_HOME/misc/zookeeper.cfg

       # Store the id of this node
       if [ "$hostname" == "$i" ] || [ "$localip" == "$i" ]; then
          instanceid=$serverid
       fi

       serverid=$((serverid+1))
    done

    # Write the id of this instance
    echo $instanceid > $zookeeper_workdir/myid
 
    # Start zookeeper
    nohup java -cp $classpath -Dzookeeper.log.dir="$logdir" org.apache.zookeeper.server.quorum.QuorumPeerMain $BBOXDB_HOME/misc/zookeeper.cfg > $logdir/zookeeper.log 2>&1 < /dev/null &
    
    if [ $? -eq 0 ]; then
       # Dump PID into file
       echo -n $! > $zookeeper_pid 
    else
       echo -e "Unable to start Zookeeper, check the logfiles for further information $failed"
       exit -1
    fi
   
  	echo -e "Zookeeper is successfully started $done"
}

###
# Zookeeper stop
###
zookeeper_stop() {
    if [ ! -f $zookeeper_pid ]; then
       echo "Unable to locate PID file"
    else
       echo "Stopping Zookeeper"
       kill -9 $(cat $zookeeper_pid)
       rm $zookeeper_pid 
       echo -e "Zookeeper is successfully stopped $done"
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
           echo -e "File $zookeeper_workdir/myid not found, skipping delete. Maybe $zookeeper_workdir is not a zookeeper workdir $failed"
           exit -1
      fi

      rm -r $zookeeper_workdir
   fi
   
   echo -e "Zookeeper data is deleted $done"
}

case "$1" in  

bboxdb_start)
   bboxdb_start
   ;;  
bboxdb_start_debug)
   BBOXDB_LOG="debug"
   bboxdb_start
   ;;
bboxdb_start_trace)
   BBOXDB_LOG="trace"
   bboxdb_start
   ;;
bboxdb_start_remote_debug)
   BBOXDB_REMOTE_DEBUG="true"
   bboxdb_start
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
zookeeper_client)
   zookeeper_client
   ;;
zookeeper_drop)
   zookeeper_drop
   ;;
*)
   echo "Usage: $0 {bboxdb_start | bboxdb_stop | bboxdb_update | zookeeper_start | zookeeper_stop | zookeeper_client | zookeeper_drop}"
   ;;  
esac

exit 0
