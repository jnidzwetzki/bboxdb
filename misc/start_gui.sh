#!/bin/bash
#
# Start the GUI of the scalephant 
#
#
#########################################

# Scriptname and Path 
pushd `dirname $0` > /dev/null
basedir=`pwd`
fullpath=$(basename $0)
popd > /dev/null

cd ..
mvn package -DskipTests
cd $basedir

# Find all jars
cd $basedir

# Does a build exists? 
if [ -d ../target ]; then
   libs=$(find ../target/lib -name '*.jar' | xargs echo | tr ' ' ':')
   jar=$(ls -1 ../target/scalephant*.jar | tail -1)
fi 

classpath="$basedir/../conf:$libs:$jar"

echo "Start the GUI"

java -cp $classpath de.fernunihagen.dna.scalephant.tools.gui.Main

exit 0
