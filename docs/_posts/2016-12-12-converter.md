---
layout: page
title: "Data converter"
category: tools
date: 2016-12-12 12:18:27
---

# Data converter

## OpenStreetMap
BBoxDB ships with an converter for OpenStreetMap (.osm.pbf - Protocolbuffer Binary Format) to GeoJSON data. The converter reads .osm.pbf files, filters certain elements and writes the matching elements to seperate files in GeoJSON format.

### Output Filter
At the moment, the converter filters the following nodes and ways:

|    Filter      |    Content    |
|----------------|---------------|
| BUILDING       | All Buildings |
| ROAD           | All Roads (road, route, way, or thoroughfare)    |
| TRAFFIC_SIGNAL | All Traffic signals (also known as traffic lights) | 
| TREE           | All important trees |
| WATER          | All lakes, ponds, rivers, canals, ... |
| WOOD           | All woods |

### Usage
The converter needs three paremter, the input file, the folder(s) for the cache databases and the output directory:

```java -server -Xmx6096m -classpath "target/*":"target/lib/*":"conf":"." org.bboxdb.performance.osm.OSMConverter <Input File> <Cache databases> <Output directory>```




