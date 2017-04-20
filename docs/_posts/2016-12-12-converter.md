---
layout: page
title: "Data converter"
category: tools
date: 2016-12-12 12:18:27
---

# OpenStreetMap
BBoxDB ships with an converter for OpenStreetMap (.osm.pbf - Protocolbuffer Binary Format) to GeoJSON data. The converter reads .osm.pbf files, filters certain elements and writes the matching elements to separate files in [GeoJSON](http://geojson.org/) format.

## Internal architecture
.osm.pbf files consist of three different structures: nodes, ways, and relations. Nodes are simply points in the space; ways have an extend and are composed of multiple nodes. Relations describe the relationship between some nodes. The converter considers only nodes and ways. All structures are enhanced with properties; i.e., a key-value map, which contains information about the object. For example the name of the object or the maximal speed for a road.

The main task of the converter is to find all required nodes for the ways. The .osm.pbf files contain all nodes at the top; the ways are stored at the bottom of the files. The files are processed in a streaming manner with the Osmosis parser. Filters are applied to the nodes and ways. If the filter matches, the node or way is written into the appropriate output file in GeoJSON format.

At the moment, the converter filters the following nodes and ways:

|    Filter      |    Content    |
|----------------|---------------|
| BUILDING       | All Buildings |
| ROAD           | All Roads (road, route, way, or thoroughfare)    |
| TRAFFIC_SIGNAL | All Traffic signals (also known as traffic lights) | 
| TREE           | All valuable trees |
| WATER          | All lakes, ponds, rivers, canals, ... |
| WOOD           | All woods |

Nodes are also stored in a Berkley DB database. Ways are consisting of multiple nodes. Before a way can be written into the output file, the corresponding nodes are fetched from the database.

## Usage
The converter requires three parameters, the input file, the folder(s) for the node databases and the output directory. 

```java -server -Xmx6096m -classpath "target/*":"target/lib/*":"conf":"." org.bboxdb.performance.osm.OSMConverter <Input File> <Databasedir1:Databasedir2:DatabasedirN> <Output directory>```

When the system consists of multiple hard disks, it is recommended to place the input and the output files on one disk and let the other disks store the node databases. It is also recommended, to increase the size of the 'memory allocation pool' of the JVM. The memory will be used as a cache for the Berley DB databases and reduce the amount disk IO.

In the following example, an extract of the OpenStreepMap database is downloaded and processed. It is assumed, that the system contains at least 32 GB of RAM (-Xmx26096m) and have four hard disks. One hard disk is used to store the input and the output data. The three remaining disks (mounted at /diskb, /diskc and /diskd) are used to store the Berkley DB databases.

```bash
wget http://download.geofabrik.de/europe/germany-latest.osm.pbf
cd $BBOXDB_HOME
java -server -Xmx26096m -classpath "target/*":"target/lib/*":"conf":"." org.bboxdb.performance.osm.OSMConverter /path/to/germany-latest.osm.pbf /diskb/work:/diskc/work:/diskd/work /outputdir/germany
```

## Example output

One line of the BUILDING file:

```json
{"geometry":{"coordinates":[[49.485530000000004,8.905184100000001],[49.4855414,8.905173300000001],[49.485554500000006,8.9051691],[49.485567800000005,8.905171900000001],[49.4855797,8.9051814],[49.4855888,8.905196400000001],[49.4855941,8.9052153],[49.485595100000005,8.905235900000001],[49.4855918,8.9052545],[49.485584900000006,8.9052707],[49.485575000000004,8.9052827],[49.4855632,8.9052892],[49.4855506,8.9052897],[49.485538600000005,8.905284],[49.485528300000006,8.9052728],[49.485520900000004,8.905257200000001],[49.485517,8.905238800000001],[49.4855171,8.9052194],[49.485521600000006,8.9052],[49.485530000000004,8.905184100000001]],"type":"Polygon"},"id":481878096,"type":"Feature","properties":{"man_made":"silo","building":"yes"}}
```

One line of the ROAD file: 

```json
{"geometry":{"coordinates":[48.1763357,11.427178000000001],"type":"Point"},"id":128963,"type":"Feature","properties":{"ref":"7","TMC:cid_58:tabcd_1:LCLversion":"8.00","TMC:cid_58:tabcd_1:NextLocationCode":"31512","name":"München-Lochhausen","TMC:cid_58:tabcd_1:LocationCode":"31954","highway":"motorway_junction","TMC:cid_58:tabcd_1:PrevLocationCode":"42385","TMC:cid_58:tabcd_1:Class":"Point","TMC:cid_58:tabcd_1:Direction":"negative"}}
```

One line of the TREE file:

```json
{"geometry":{"coordinates":[52.9744383,8.630228],"type":"Point"},"id":31339954,"type":"Feature","properties":{"natural":"tree"}}
```