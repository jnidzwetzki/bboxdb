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

# The base url
baseurl="http://download.geofabrik.de/europe/"

# outputfolder
outputfolder="/BIG/nidzwetzki/tmp/work"
workfolder="/SSD/nidzwetzki/tmp"

if [ ! -d $outputfolder ]; then
    mkdir $outputfolder
fi

cd $outputfolder

for line in $(wget -O - $baseurl | grep 'latest.osm.pbf"'); do 
   filename=$(echo $line | grep .osm.pbf | cut -d '"' -f 2)
   region=$(echo $filename | sed s/-latest.osm.pbf//g)

   if [ -z $region ]; then
     continue
   fi

   echo "Processing $region" 

   if [ -d $workfolder ]; then
       rm -r $workfolder
   fi

   mkdir $workfolder

   # Download the region if needed
   if [ ! -d $region ]; then
      mkdir $region
      cd $region
      echo "Downloading region $region"
      wget $baseurl/$filename

      $BBOXDB_HOME/bin/osm_data_converter.sh -input $outputfolder/$region/$filename -backend bdb -workfolder $workfolder -output $outputfolder/$region/osm

      lines=$(wc -l $outputfolder/$region/osm/ROAD | cut -d " " -f 1)
      sample_lines=$(($lines / 100))
	  echo "Roads have $lines lines (samples $sample_lines)"
      shuf -n $sample_lines $outputfolder/$region/osm/ROAD > $outputfolder/$region/osm/ROAD_SAMPLES

      cd ..
   fi
done

