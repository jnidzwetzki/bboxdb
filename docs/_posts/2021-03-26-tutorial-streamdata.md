---
layout: page
title: "Tutorial: Data Streams"
category: doc
date: 2021-03-26 12:18:27
order: 5
---

# Handle a real-world data stream with BBoxDB Streams

This tutorial shows, how you can process a real-world data set of position data with BBoxDB Streams. Queries such as spatial joins between the stream elements and n-dimensional data will be performed. The position data of public transport vehicles in Sydney are used as data stream. Spatial data from the OpenStreetMap project is used for the static dataset. 

## Download and Convert Open Street Map Data into GeoJSON
https://ftp5.gwdg.de/pub/misc/openstreetmap/planet.openstreetmap.org/pbf

http://download.geofabrik.de/

$BBOXDB_HOME/bin/osm_data_converter.sh -input $datadir/north-america-latest.osm.pbf -backend bdb -workfolder /BIG/nidzwetzki/work -output $datadir/converted


# Prepartition the Space and Import the GeoJSON Data
$BBOXDB_HOME/bin/import_osm.sh /BIG/nidzwetzki/datasets/osm/australia/converted nowait

# Create an Account to Access the Data Stream

https://opendata.transport.nsw.gov.au/

# Import the Datastream
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_lightrail
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_buses
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_metro
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_nswtrains
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_ferries
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_trains


$BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.network.ImportAuTransport "<Your-API-Key>" lightrail:buses:metro:nswtrains:ferries:trains newton1:50181 mycluster osmgroup 2


# Perform Queries on the Data Stream


