---
layout: page
title: "Tutorial: Data Streams"
category: doc
date: 2021-03-26 12:18:27
order: 5
---


# Handle a Real-World Stream of Public Transport Data

This tutorial shows how you can process a real-world data stream of position data with BBoxDB Streams. Continuous queries such as range queries or spatial joins between the stream elements and n-dimensional data will be performed. The position data of public transport vehicles in Sydney are used as the real-world data stream. The data stream can be fetched from the open data website of the public transport company in [New South Wales](https://opendata.transport.nsw.gov.au/). Spatial data from the [OpenStreetMap project](https://www.openstreetmap.org) is used for the static dataset. Queries such as:

* _Which bus / train / ferry is currently located in a given query rectangle_ (continuous range query)?
* _Which bus is currently located on a Bridge_ (continuous spatial join query)?
* _Which bus is currently driving through a forest_ (continuous spatial join query)?
* _Which bus is currently located on a particular road_ (continuous spatial join query)?


<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_sydney.jpg"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_sydney.jpg" width="400"></a>
<br>
<font size="small">The buses in Sydney, interactively shown as a continuous range query in the GUI of BBoxDB.</font>
</div>

__Note:__ For more information, have a look at our Stream Processing [paper](https://edbt2021proceedings.github.io/docs/p170.pdf), presented at EDBT 2021.

## Download and Convert Open Street Map Data into GeoJSON

For performing the continuous spatial joins, you need to import the spatial dataset of the area first. Please download the complete [Planet dataset](https://ftp5.gwdg.de/pub/misc/openstreetmap/planet.openstreetmap.org/pbf) or the [Australia dataset](http://download.geofabrik.de/) in `.osm.pbf` format.

After the dataset is downloaded, it needs to be converted into GeoJSON elements. This can be done by calling the following command:

```
$BBOXDB_HOME/bin/osm_data_converter.sh -input <your-dataset>.osm.pbf -backend bdb -workfolder /tmp/work -output <outputdir>
```

After the command finishes, several files in the output folder like `ROADS` or `FORSTS` are generated. These files contain the spatial data of the corresponding OpenStreetMap elements as GeoJSON elements. Each like of the file contains one GeoJSON element. For example, one entry might look like (the entry is formatted and split-up into multiple lines for improved reading):

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

## Pre-partition the Space and Import the GeoJSON Data

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

## Create an Account to Access the Data Stream

To fetch the data stream of the vehicles in Sydney, you have to apply for a API key. This can be done at the following [website](https://opendata.transport.nsw.gov.au/). Please create an API key that is capable of accessing the "GTFS real-time" encoded data stream of the vehicles.

## Import the Datastream

To import the data stream, the following tables need to be created in BBoxDB. In these tables, the data stream elements will be stored. All tables are part of the distribution group `osm`.

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

## Perform Queries on the Data Stream

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
<font size="small">The predefined queries in the GUI.</font>
</div>

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_forest_joined_with_bus.png"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_forest_joined_with_bus.png" width="400"></a>
<br>
<font size="small">A bus in a forest.</font>
</div>

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_bus_on_bridge.png"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_gui_bus_on_bridge.png" width="400"></a>
<br>
<font size="small">A bus on a bridge.</font>
</div>


# Handle a Real-World Stream of ADS-B Data

In this part of the tutorial, a stream of [ADS-B data](https://de.wikipedia.org/wiki/Automatic_Dependent_Surveillance) (Automatic Dependent Surveillance-Broadcast) is processed with BBoxDB Streams. ADS-B data contains the position data of aircraft. The GUI of BBoxDB is used to show live data of aircraft (containing the position, height, heading, call sign, and much more). 

In this tutorial, the data is fetched from two input sources:

* A local ADS-B receiver
* The website [ADSBHub.org](https://www.adsbhub.org/). 

To capture ADS-B data, an AirNav USB-Stick is used. This is a small USB-receiver that is delivered together with an antenna. The receiver can be bought at websites such as [Amazon](https://www.amazon.de/gp/product/B07K47P7XD).

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/adsb_receiver.jpg"><img src="https://jnidzwetzki.github.io/bboxdb/images/adsb_receiver.jpg" width="400"></a>
<br>
<font size="small">A receiver that is capable of capturing ADS-B messages.</font>
</div>

The local ADS-B receiver captures the data of aircraft in the region of the antenna. Unfortunately, the antenna can only capture transmissions in a radius of a few miles. To get an ADS-B data stream covering a larger region, websites such as adsbhub.org can be used. By uploading your received data, you can fetch the data from all registered stations. 

To access this data stream of adsbhub.org, you have to upload your received ADS-B data stream to adsbhub.org website. Afterward, you have access to the data of all other stations that are transmitting data to adsbhub.org.

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/adsb_coverage.png"><img src="https://jnidzwetzki.github.io/bboxdb/images/adsb_coverage.png" width="400"></a>
<br>
<font size="small">The coverage of the adsbhub.org website.</font>
</div>



## Install the Needed Software to Handle the ADS-B data

After the ADS-B receiver is connected to your PC, you need to download and compile the program `dump1090`. The program is capable of decoding ADS-B data.

```
git clone https://github.com/flightaware/dump1090.git
cd dump1090
apt-get install librtlsdr-dev
make
```

## Execute dump1090

Some Linux distributions load some kernel drivers automatically for the USB stick. These drivers prevent that `dump1090` can access the receiver directly. Please unload the driver first to ensure that `dump1090` works correctly.

```
rmmod rtl2832 rtl2832_sdr rtl2832 dvb_core dvb_usb_v2 dvb_usb_rtl28xxu
```

Afterward, `dump1090` can be started, by executing:

```
./dump1090 --interactive --device-type rtlsdr --net
```

The output for the program should look as follows. In the example, eight aircraft are shown. 

```
 Tot:   8 Vis:   8 RSSI: Max -26.0+ Mean -31.5 Min -34.7-  MaxD:    0.0nm+     /
 Hex    Mode  Sqwk  Flight   Alt    Spd  Hdg    Lat      Long   RSSI  Msgs  Ti
────────────────────────────────────────────────────────────────────────────────
 3CCCAA A0    5004           12750  373  072                   -32.6    44 10
 4BA8C5 A2    1000  THY2SY    4275  208  248   52.458   13.866 -31.2   156 51
 3944F3 A0    1000  AFR91RW  38975  473  073                   -32.8    27 23
 3D6275 A2          DFJNP     9800  151  259   52.037   13.088 -32.0    88 04
 4BCDE3 A0                                                     -34.7-    4 42
 5140A8 A0    3213           34325  382  256   52.148   15.322 -32.4   332 11
 3C64A7 A2    1000  DLH8RJ   13175  349  233   52.185   13.021 -26.0+ 2350 08
 3D2C04 S     7372  DEOXO     1200                             -30.7  1352 58
```

## Upload the Captured Data to adsbhub.org

The program `dump1090` was started in network mode by specifying the `--net` parameter. This means that the program opens several ports and provides the data stream in various formats on these ports. By calling the following command, the data stream is read from the port `30003`.

```
nc localhost 30003
```

The command should show an output that looks like the following example. This is the ADS-B data encoded in SBS-format. More about this data format can be found at the following [website](http://woodair.net/sbs/article/barebones42_socket_data.htm).

```
MSG,5,1,1,3D2C04,1,2021/03/31,10:59:34.052,2021/03/31,10:59:34.087,,1600,,,,,,,0,,0,
MSG,8,1,1,471F8C,1,2021/03/31,10:59:34.141,2021/03/31,10:59:34.195,,,,,,,,,,,,0
MSG,8,1,1,471F8C,1,2021/03/31,10:59:34.177,2021/03/31,10:59:34.200,,,,,,,,,,,,0
MSG,8,1,1,471F8C,1,2021/03/31,10:59:34.186,2021/03/31,10:59:34.201,,,,,,,,,,,,0
MSG,8,1,1,471F8C,1,2021/03/31,10:59:34.187,2021/03/31,10:59:34.201,,,,,,,,,,,,0
MSG,5,1,1,3D2C04,1,2021/03/31,10:59:34.309,2021/03/31,10:59:34.359,,1600,,,,,,,0,,0,
MSG,7,1,1,5140A8,1,2021/03/31,10:59:34.523,2021/03/31,10:59:34.577,,37500,,,,,,,,,,
MSG,4,1,1,3C64A7,1,2021/03/31,10:59:34.637,2021/03/31,10:59:34.687,,,244,235,,,2368,,,,,0
MSG,5,1,1,3D2C04,1,2021/03/31,10:59:35.001,2021/03/31,10:59:35.020,,1600,,,,,,,0,,0,
MSG,5,1,1,3D2C04,1,2021/03/31,10:59:35.052,2021/03/31,10:59:35.073,,1600,,,,,,,0,,0,
MSG,7,1,1,3C64A7,1,2021/03/31,10:59:35.353,2021/03/31,10:59:35.398,,6725,,,,,,,,,,
MSG,8,1,1,3C64A7,1,2021/03/31,10:59:35.456,2021/03/31,10:59:35.506,,,,,,,,,,,,0
MSG,8,1,1,3C64A7,1,2021/03/31,10:59:35.474,2021/03/31,10:59:35.509,,,,,,,,,,,,0
MSG,8,1,1,3C64A7,1,2021/03/31,10:59:35.502,2021/03/31,10:59:35.512,,,,,,,,,,,,0
MSG,6,1,1,471F8C,1,2021/03/31,10:59:35.559,2021/03/31,10:59:35.566,,,,,,,,0514,0,0,0,
MSG,3,1,1,3C64A7,1,2021/03/31,10:59:35.671,2021/03/31,10:59:35.724,,6725,,,52.30884,13.29165,,,0,,0,0
MSG,4,1,1,3C64A7,1,2021/03/31,10:59:35.676,2021/03/31,10:59:35.725,,,246,235,,,2368,,,,,0
MSG,8,1,1,3C64A7,1,2021/03/31,10:59:35.867,2021/03/31,10:59:35.889,,,,,,,,,,,,0
MSG,5,1,1,3C64A7,1,2021/03/31,10:59:35.870,2021/03/31,10:59:35.889,,6750,,,,,2208,,0,,0,
MSG,5,1,1,3C64A7,1,2021/03/31,10:59:35.888,2021/03/31,10:59:35.942,,6750,,,,,,,0,,0,
```

``` 
wget https://raw.githubusercontent.com/jnidzwetzki/bboxdb/master/misc/upload_to_adsbhub.sh
chmod +x ./upload_to_adsbhub.sh 
./upload_to_adsbhub.sh 
```

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/adsb_data.png"><img src="https://jnidzwetzki.github.io/bboxdb/images/adsb_data.png" width="400"></a>
<br>
<font size="small">Your station data at the website adsbhub.org.</font>
</div>

You can verify that you can download the ADS-B data stream by executing:

```
nc data.adsbhub.org 5002
```

The command should show you the current ads-b data stream of adsbhub.org. The data stream looks like this and contains the data of airplanes of the whole world.

```
MSG,3,0,0,E495D8,0,2021/03/31,09:35:22.000,2021/03/31,09:35:22.000,,38000,,,-22.659973,-44.255676,,,,,,
MSG,4,0,0,E495D8,0,2021/03/31,09:35:22.000,2021/03/31,09:35:22.000,,,400.870300,249.711609,,,0,,,,,
MSG,1,0,0,E495DF,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,AZU4399,,,,,,,,,,,
MSG,3,0,0,E495DF,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,,15650,,,-22.554810,-46.767151,,,,,,
MSG,4,0,0,E495DF,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,,,313.209198,216.430862,,,-1216,,,,,
MSG,1,0,0,E495F9,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,GLO1547,,,,,,,,,,,
MSG,3,0,0,E495F9,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,,11950,,,-23.055298,-46.552124,,,,,,
MSG,4,0,0,E495F9,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,,,293.163788,199.114807,,,-1472,,,,,
MSG,1,0,0,E4966D,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,AZU4514,,,,,,,,,,,
MSG,3,0,0,E4966D,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,,41000,,,-25.244934,-47.058411,,,,,,
MSG,4,0,0,E4966D,0,2021/03/31,09:35:22.000,2021/03/31,09:35:21.000,,,483.359070,35.399883,,,64,,,,,
```

## Prepare BBoxDB and Import the Data Stream

After the data stream can be fetched, it's time to prepare BBoxDB to handle the data stream. In the first step, the needed distribution group (`osmgroup` in this example) is created together with the table for the adsb data `osmgroup_adsb`.

```
$BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup osmgroup -dimensions 2 -maxregionsize 1024
$BBOXDB_HOME/bin/cli.sh -action create_table -table osmgroup_adsb
```

After BBoxDB is prepared, the import of the ADS-B data stream can be performed by executing the following command. The command fetches the data stream and converts the ADS-B data in the SBS format into GeoJSON elements, which can be processed later by the GUI of BBoxDB.

```
$BBOXDB_HOME/bin/bboxdb_execute.sh org.bboxdb.tools.network.ImportADSB <cluster-contact-point> <your-cluster-name> osmgroup_adsb
```

## Perform Queries on The GUI

So, the data stream is available in BBoxDB; queries on the data can now be performed. This can be done by the GUI of BBoxDB. You can start the GUI by executing:

```
$BBOXDB_HOME/bin/gui.sh
```

Open the `query view` on the GUI and perform range queries or continuous range queries on the __osmgroup_adsb__ table.

<div align="center" style="padding-bottom:20px">
<a href="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_adsb.jpg"><img src="https://jnidzwetzki.github.io/bboxdb/images/bboxdb_adsb.jpg" width="400"></a>
<br>
<font size="small">A continuous range query on the ADS-B data stream in the area of Berlin, shown in the GUI of BBoxDB.</font>
</div>