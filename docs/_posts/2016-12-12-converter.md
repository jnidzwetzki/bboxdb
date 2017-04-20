---
layout: page
title: "Data converter"
category: tools
date: 2016-12-12 12:18:27
---

# OpenStreetMap
BBoxDB ships with an converter for OpenStreetMap (.osm.pbf - Protocolbuffer Binary Format) to GeoJSON data. The converter reads .osm.pbf files, filters certain elements and writes the matching elements to seperate files in GeoJSON format.

## Output Filter
At the moment, the converter filters the following nodes and ways:

|    Filter      |    Content    |
|----------------|---------------|
| BUILDING       | All Buildings |
| ROAD           | All Roads (road, route, way, or thoroughfare)    |
| TRAFFIC_SIGNAL | All Traffic signals (also known as traffic lights) | 
| TREE           | All important trees |
| WATER          | All lakes, ponds, rivers, canals, ... |
| WOOD           | All woods |

## Internal architecture


## Usage
The converter needs three paremter, the input file, the folder(s) for the cache databases and the output directory:

```java -server -Xmx6096m -classpath "target/*":"target/lib/*":"conf":"." org.bboxdb.performance.osm.OSMConverter <Input File> <Cache databases> <Output directory>```

## Example

###One example of the BUILDING file:

```json
{"geometry":{"coordinates":[[49.485530000000004,8.905184100000001],[49.4855414,8.905173300000001],[49.485554500000006,8.9051691],[49.485567800000005,8.905171900000001],[49.4855797,8.9051814],[49.4855888,8.905196400000001],[49.4855941,8.9052153],[49.485595100000005,8.905235900000001],[49.4855918,8.9052545],[49.485584900000006,8.9052707],[49.485575000000004,8.9052827],[49.4855632,8.9052892],[49.4855506,8.9052897],[49.485538600000005,8.905284],[49.485528300000006,8.9052728],[49.485520900000004,8.905257200000001],[49.485517,8.905238800000001],[49.4855171,8.9052194],[49.485521600000006,8.9052],[49.485530000000004,8.905184100000001]],"type":"Polygon"},"id":481878096,"type":"Feature","properties":{"man_made":"silo","building":"yes"}}
```

###One example line of the ROAD file: 
```json
{"geometry":{"coordinates":[48.1763357,11.427178000000001],"type":"Point"},"id":128963,"type":"Feature","properties":{"ref":"7","TMC:cid_58:tabcd_1:LCLversion":"8.00","TMC:cid_58:tabcd_1:NextLocationCode":"31512","name":"München-Lochhausen","TMC:cid_58:tabcd_1:LocationCode":"31954","highway":"motorway_junction","TMC:cid_58:tabcd_1:PrevLocationCode":"42385","TMC:cid_58:tabcd_1:Class":"Point","TMC:cid_58:tabcd_1:Direction":"negative"}}
```

###One example line of the TREE file:
```json
{"geometry":{"coordinates":[52.9744383,8.630228],"type":"Point"},"id":31339954,"type":"Feature","properties":{"natural":"tree"}}
```

