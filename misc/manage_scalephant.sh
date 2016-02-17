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
classpath="$libs:$jar"



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
*)
   echo "Usage: $0 {start|stop|update}"
   ;;  
esac

exit 0
