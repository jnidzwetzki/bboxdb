---
layout: page
title: "Tutorial: Data Streams"
category: doc
date: 2021-03-26 12:18:27
order: 5
---

# Handle a real-world data stream with BBoxDB Streams

This tutorial shows how you can process a real-world data set of position data with BBoxDB Streams. Queries such as spatial joins between the stream elements and n-dimensional data will be performed. The position data of public transport vehicles in Sydney are used as a data stream. Spatial data from the OpenStreetMap project is used for the static dataset. Queries such as:

* _Which bus / train / ferry is currently located in a given query rectangle_ (continuous range query)?
* _Which bus is currently located on a Bridge_ (continuous spatial join query)?
* _Which bus is currently driving through a forest_ (continuous spatial join query)?
* _Which bus is currently located on a particular road_ (continuous spatial join query)?

<div align="center">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_sydney.jpg"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_sydney.jpg" width="400"></a>
</div>

## Download and Convert Open Street Map Data into GeoJSON

For performing the continuous spatial joins, you need to import the spatial dataset of the area first. Please download the complete [Planet dataset](https://ftp5.gwdg.de/pub/misc/openstreetmap/planet.openstreetmap.org/pbf) or the [Australia dataset](http://download.geofabrik.de/) in `.osm.pbf` format.

After the dataset is downloaded, it needs to be converted into GeoJSON elements. This can be done by calling the following command:

```
$BBOXDB_HOME/bin/osm_data_converter.sh -input <your-dataset>.osm.pbf -backend bdb -workfolder /tmp/work -output <outputdir>
```

After the command finishes, you have serveral files in the output folder like `ROADS` or `FORSTS`. These files contains the spatial data of the corresponding OpenStreetMap elements as GeoJSON elements. Each like of the file contains one GeoJSON element. For example, one entry might look like (the entry is formatted for improved reading):

```json
{
   "geometry":{
      "coordinates":[
         [
            151.2054938,
            -33.9045641
         ],
         [
            151.2056594,
            -33.9047744
         ],
         [
            151.20597560000002,
            -33.905176000000004
         ],
         [
            151.2063965,
            -33.9057107
         ],
         [
            151.20641930000002,
            -33.905739700000005
         ]
      ],
      "type":"LineString"
   },
   "id":756564602,
   "type":"Feature",
   "properties":{
      "surface":"paved",
      "hgv":"destination",
      "maxspeed":"40",
      "name":"Elizabeth Street",
      "highway":"residential",
      "maxweight":"3"
   }
}
```

See [this page](https://jnidzwetzki.github.io/bboxdb/tools/dataset.html) for more information about the data converter. 

# Prepartition the Space and Import the GeoJSON Data

After the spatial data is converted into GeoJSO, you can import the data by calling the following command:

```
$BBOXDB_HOME/bin/import_osm.sh <outputdir> nowait
```

The command performs the following tasks:

* The distribution group `osm` (short for OpenStreetMap) is created.
* The tables `osm_road` and `osm_forst` are created.
* A sample is taken from the data, and the space is pre-partitioned into 10 distribution regions. 
* The spatial data is read and imported into BBoxDB.

__Hint__: When you remote the `nowait` parameter from the command, the command will stop after each step, and you can analyze the output.

# Create an Account to Access the Data Stream

To fetch the data stream of the vehicles in Sydney, you have to apply for a API key. This can be done at the following [website](https://opendata.transport.nsw.gov.au/). Please create an API key that is capable of accessing the "GTFS real-time" encoded data stream of the vehicles.

# Import the Datastream

To import the data stream, the following tables need to be created in BBoxDB.

```
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_lightrail
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_buses
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_metro
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_nswtrains
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_ferries
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_trains
```

Afterward, you can start the import of the data stream into BBoxDB. 

```
$BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.network.ImportAuTransport "<Your-API-Key>" lightrail:buses:metro:nswtrains:ferries:trains <cluster-contact-point> <your-cluster-name> osmgroup 2
```

__Note__: The `2` at the end of the command means that the data source is pulled every 2 seconds, and the data is imported into BBoxDB.

# Perform Queries on the Data Stream

On the CLI, you can perform the following continuous query to see all data of the data stream. The provided bounding box for the range query `-35:-30:150:152` covers the area of Australia.

```
$BBOXDB_HOME/bin/cli.sh -action query_continuous -table osmgroup_buses -bbox -35:-30:150:152
```

For more queries, please use the GUI of BBoxDB. You can start the GUI by executing:

```
$BBOXDB_HOME/bin/gui.sh
```

Open the `query view` on the GUI and navigate to Sydney. In the GUI the queries from the introduction are pre-defined. In addition, you can execute individual queries.


<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_predefined_queries.png"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_predefined_queries.png" width="400"></a>
<br>
<font size="small">The predefined queries in the GUI</font>
</div>

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_forest_joined_with_bus.png"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_forest_joined_with_bus.png" width="400"></a>
<br>
<font size="small">A bus in a forest</font>
</div>

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_bus_on_bridge.png"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_bus_on_bridge.png" width="400"></a>
<br>
<font size="small">A bus on a bridge</font>
</div>


<br>

__Note:__ For more information, have a look at our Stream Processing [paper](https://edbt2021proceedings.github.io/docs/p170.pdf), presented at EDBT 2021.