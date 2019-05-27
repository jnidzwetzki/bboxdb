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
# This script converts GeoJSON data into WKT
######


# Environment
export GDAL_HOME="~/gdal/gdal-2.1.1"
export GDAL_DATA=$GDAL_HOME/data

# Max lines per chunk
maxlines=5000

if [ ! -d $GDAL_HOME ]; then
   echo "GDAL dir ($GDAL_HOME) does not exist, please check your installation"
   exit -1
fi

if [ "$#" -ne 3 ]; then
    echo "Usage <Input> <Temp-Dir> <Output>"
    exit -1
fi

input=$1
tmp=$2
output=$3

echo "Converting $input to $output via $tmp"

if [ -r $tmp ]; then
    echo "Error: Temp dir $tmp already exists, please remove"
    exit -1
fi

if [ -r $output ]; then
    echo "Error: Output $output already exists, please remove"
    exit -1
fi

if [ ! -r $input ]; then
    echo "Error: Input file does not exist"
    exit -1
fi

mkdir $tmp
cp -av $input $tmp
cd $tmp

inputfile=$(basename $input)

split -l $maxlines -a 10 -d $inputfile data. 

for file in data.*; do
   echo "Processing $file"
   sed -e 's/$/,/' -i $file
   sed -i '1s/^/{"type": "FeatureCollection", "features": [\n/' $file
   echo -e "\n]}" >> $file
   $GDAL_HOME/apps/ogr2ogr -f CSV result $file  -lco GEOMETRY=AS_WKT -lco SEPARATOR=TAB 2> error.log
   egrep 'POINT|LINE|POLYGON' result/OGRGeoJSON.csv >> $output
   rm -rf result
done

sed 's/"//g' -i $output

rm -r $tmp

