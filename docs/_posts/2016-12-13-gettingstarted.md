---
layout: page
title: "Getting started"
category: doc
date: 2016-12-12 22:46:12
order: 1
---

# An Example with OpenStreetmap Data
This section covers an example to demonstrate the basic functionality of BBoxDB. Data of the OpenStreetmap project is fechted, converted and imported into a table. Then, some queries are executed on this data set. In this example, the spatial data of Germany is used. Then, all trees around the famous [Alexanderplatz](https://en.wikipedia.org/wiki/Alexanderplatz) (a public square in Berlin) are fetched.

## Viewing the discovered BBoxDB instances
Before the data is imported, it is useful to verify that all started instances of BBoxDB are discovered and ready. This task can be done via the 'show_instances' action of the CLI.

```bash
$ $BBOXDB_HOME/bin/cli.sh -action show_instances
Connecting to BBoxDB cluster... [Established]
Show all discovered BBoxDB instances

#######
DistributedInstance [ip=192.168.1.181, port=50505, version=0.3.1, state=READY]
DistributedInstance [ip=192.168.1.189, port=50505, version=0.3.1, state=READY]
DistributedInstance [ip=192.168.1.192, port=50505, version=0.3.1, state=READY]
DistributedInstance [ip=192.168.1.199, port=50505, version=0.3.1, state=READY]
DistributedInstance [ip=192.168.1.202, port=50505, version=0.3.0, state=UNKNOWN]
#######
```

In our example, the cluster consists of 5 BBoxDB instances. The instances on the first four systems are ready. The instance of the system 192.168.1.202 is not available at the moment.

## Downloading and Converting Data
The first step is to download and to convert the data set. The data is stored in a format called 'Protocolbuffer Binary Format'. BBoxDB ships with a converter that converts this format into GeoJSON. You will find more details about the data converter [here](/bboxdb/tools/converter.html).

The conversion can be executed with the following commands:
```bash
$ wget http://download.geofabrik.de/europe/germany-latest.osm.pbf
$ cd $BBOXDB_HOME
$ $BBOXDB_HOME/bin/osm_data_conveter.sh -input /path/to/germany-latest.osm.pbf -backend bdb -workfolder /tmp -output /path/to/outputdir/germany
```

After the conversion has finished, the data in the output directory looks like this:

```bash
$ ls -l /path/to/outputdir/germany
-rw-rw-r-- 1 nidzwetzki nidzwetzki 7520947178 Apr 21 23:18 BUILDING
-rw-rw-r-- 1 nidzwetzki nidzwetzki 3850151857 Apr 21 23:18 ROAD
-rw-rw-r-- 1 nidzwetzki nidzwetzki   26251267 Apr 21 23:18 TRAFFIC_SIGNAL
-rw-rw-r-- 1 nidzwetzki nidzwetzki  184394469 Apr 21 23:18 TREE
-rw-rw-r-- 1 nidzwetzki nidzwetzki  159681265 Apr 21 23:18 WATER
-rw-rw-r-- 1 nidzwetzki nidzwetzki   82794367 Apr 21 23:18 WOOD
```

The input is split up into multiple files and converted into GeoJSON. The file 'BUILDING' (20,828,427 objects) contains all buildings in Germany; the file 'TREE' contains all trees (1,118,516 objects) of Germany.

##  Importing Data
The file 'TREE' is now imported into BBoxDB. However, a 2-dimensional distribution group needs to be created first. The distribution group in this example is called 'mydgroup'. The data of the trees will be stored in the table 'mydgroup_germanytree'.

The tasks can be accomplished with the following two commands:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup mydgroup -replicationfactor 2 -dimensions 2
$ $BBOXDB_HOME/bin/cli.sh -action create_table -table mydgroup_germanytree
$ $BBOXDB_HOME/bin/cli.sh -action import -file /path/to/TREE -format geojson -table mydgroup_germanytree
```

## Fetching Data
Now, the stored data can be accessed. The data importer uses a consecutive number for each object. Therefore, to fetch the object with the key '120', the following command can be used:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_germanytree -key 120

Connecting to BBoxDB cluster... [Established]
Executing key query..
Key 120, BoundingBox=[52.546123300000005:52.546123300000005,13.350283200000002:13.350283200000002], value={"geometry":{"coordinates":[52.546123300000005,13.350283200000002],"type":"Point"},"id":405400527,"type":"Feature","properties":{"natural":"tree","leaf_cycle":"deciduous","leaf_type":"broadleaved"}}, version timestamp=1493788229600008
Query done
```

The tuple is loaded from BBoxDB and printed on the console. The Key, the Bounding Box and the GeoJSON data (the value) of the tuple are printed. The area around the Alexanderplatz can be roughly expressed by a square with the following coordinates: 13.410, 52.520 for the lower left corner and 13.415, 52.525 for the upper right corner. The following command can be used, to fetch all trees, which lie inside of the square:

```bash
$BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_germanytree -bbox 13.410:52.520,13.415:52.525

[...]
Key 37587, BoundingBox=[52.4558036:52.4558036,13.4450991:13.4450991], value={"geometry":{"coordinates":[52.4558036,13.4450991],"type":"Point"},"id":3451433771,"type":"Feature","properties":{"natural":"tree","leaf_cycle":"deciduous","leaf_type":"broadleaved"}}, version timestamp=1493788236276020
Key 37588, BoundingBox=[52.455812:52.455812,13.440128000000001:13.440128000000001], value={"geometry":{"coordinates":[52.455812,13.440128000000001],"type":"Point"},"id":3451433774,"type":"Feature","properties":{"natural":"tree","leaf_cycle":"deciduous","leaf_type":"broadleaved"}}, version timestamp=1493788236276022
Key 37589, BoundingBox=[52.455847000000006:52.455847000000006,13.446559800000001:13.446559800000001], value={"geometry":{"coordinates":[52.455847000000006,13.446559800000001],"type":"Point"},"id":3451433775,"type":"Feature","properties":{"natural":"tree","leaf_cycle":"deciduous","leaf_type":"broadleaved"}}, version timestamp=1493788236276024
Query done
```

## Viewing the Data Distribution

BBoxDB distributes the data of a distribution group across multiple systems. Depending on the configuration, the imported data could be already spread across several systems. This can be viewed via CLI or via GUI.

### Via the Command Line Interface (CLI)

To view how the data of the distribution group mydgroup is spread, the following command can be used:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action show_dgroup -dgroup mydgroup

Region 0, Bounding Box=Dimension:0 [min,max], Dimension:1 [min,max], State=SPLIT, Systems=[192.168.1.183:50505, 192.168.1.191:50505]

Region 1, Bounding Box=Dimension:0 [min,52.5145621], Dimension:1 [min,max], State=ACTIVE, Systems=[192.168.1.189:50505, 192.168.1.192:50505]

Region 2, Bounding Box=Dimension:0 (52.5145621,max], Dimension:1 [min,max], State=ACTIVE, Systems=[192.168.1.181:50505, 192.168.1.199:50505]

[....]
```

It can be seen, that the data of the region 0 is split. Region 1 stores the data that belongs to the bounding box Dimension:0 [min,52.5145621], Dimension:1 [min,max]. Region 2 stores the remaining data. The  region 1 is stored on the systems '192.168.1.189:50505, 192.168.1.192:50505' the region 2 is stored on the systems '192.168.1.181:50505, 192.168.1.199:50505'.

### Via the Graphical User Interface (GUI)
To use the GUI, please use the following command:

```bash
$ $BBOXDB_HOME/bin/gui.sh
```

<p><img src="/bboxdb/images/bboxdb_gui1.jpg" width="400"></p>

After connecting to BBoxDB, the GUI shows all discovered distribution groups on the left side. On the bottom, all known BBoxDB-nodes and their state are shown. In the middle of the GUI, the K-D Tree of the distribution group is printed. For two-dimensional distribution groups which work with WGS84 coordinates, an overlay for OpenStreetMap data can be displayed.

<p><img src="/bboxdb/images/bboxdb_gui2.jpg" width="400"></p>

# Working with the tuple history
BBoxDB can store a history for each tuple. The history stores a certain amount of old values for each tuple. To use this function, the corresponding table must be allowed to contain duplicates for keys. To prevent the tables from becoming infinitely large, BBoxDB can automatically delete old tuples. This is done, when more then a certain amount of _versions_ for a key contained in the table or then the tuples become older than a certain amount of time (_time to live_).

In the following example, a table is created that contains up to three versions for a certain key:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup mydgroup -replicationfactor 1 -dimensions 2
$ $BBOXDB_HOME/bin/cli.sh -action create_table -table mydgroup_data -duplicates true -versions 3
```
Now five versions for the key are inserted with the values _value1_, _value2_, _value3_, _value4_ and _value5_:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key1 -bbox 1:2,1:2 -value value1
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key1 -bbox 1:2,1:2 -value value2
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key1 -bbox 1:2,1:2 -value value3
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key1 -bbox 1:2,1:2 -value value4
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key1 -bbox 1:2,1:2 -value value5
```

The key query returns the three most recent versions for the key:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_data -key key1 
Connecting to BBoxDB cluster... [Established]
Executing key query..
Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value3, version timestamp=1509699605215000
Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value4, version timestamp=1509699607483000
Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value5, version timestamp=1509699609644000
Query done
```

The query retuns three different versions for the key _key1_. The tuples are sorted by the _version timestamp_ and the most recent version of the tuple (with the value of _value5_) is shown at the last postion. 

Now, the tuple for the key is deleted and the key query is executed again. It can be seen that there are still three versions stored for this key. The version with the value "value3" has been removed and a new version (which says that the tuple was deleted) has been added.

```bash
$ $BBOXDB_HOME/bin/cli.sh -action delete -table mydgroup_data -key key1
$ $BBOXDB_HOME/bin/cli.sh -action query -table mydgroup_data -key key1 
Connecting to BBoxDB cluster... [Established]
Executing key query..
Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value4, version timestamp=1509699607483000
Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value5, version timestamp=1509699609644000
Key key1, DELETED, version timestamp=1509703784058000
Query done
```

# Working with continuous bounding box queries
BBoxDB supports continuous bounding box queries. As soon as the query is executed, all tuples that are inserted within the bounding box are captured by this query. To demonstrate this, the continuous bounding box query is started on one console:

```bash
console1> $BBOXDB_HOME/bin/cli.sh -action continuous-query -table mydgroup_data -bbox 0:5,0:5
Connecting to BBoxDB cluster... [Established]
Executing continuous bounding box query...
```

On another console, a new tuple is inserted in the query bounding box:

```bash
# Tuple (bbox completely contained in query)
console2> $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key1 -bbox 1:2,1:2 -value value1

# Tuple (bbox not contained in query)
console2> $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key2 -bbox 10:15,10:15 -value value2

# Tuple (bbox partially contained in query)
console2> $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_data -key key3 -bbox 2:10,2:10 -value value3
```

As soon as the three tuples are inserted, the query reports two tuples:

```bash
console1> $BBOXDB_HOME/bin/cli.sh -action continuous-query -table mydgroup_data -bbox 0:5,0:5
Connecting to BBoxDB cluster... [Established]
Executing continuous bounding box query...
Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value1, version timestamp=1510325620579000
Key key3, BoundingBox=[2.0:10.0,2.0:10.0], value=value3, version timestamp=1510325620643000
```

# Executing a join
In this example, a join on two tables is executed. All tuples of the tables `mydgroup_table1` and `mydgroup_table2` are reported by the join query. First of all, both tables are created and some tuples are inserted:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action create_dgroup -dgroup mydgroup -replicationfactor 1 -dimensions 2
$ $BBOXDB_HOME/bin/cli.sh -action create_table -table mydgroup_table1
$ $BBOXDB_HOME/bin/cli.sh -action create_table -table mydgroup_table2

# Tuples of table1
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table1 -key key1 -bbox 1:2,1:2 -value value1
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table1 -key key2 -bbox 5:7,5:7 -value value2
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table1 -key key3 -bbox 10:12,10:12 -value value3

# Tuples of table2
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table2 -key key1 -bbox 1:20,1:20 -value value1
$ $BBOXDB_HOME/bin/cli.sh -action insert -table mydgroup_table2 -key key2 -bbox 1:3,1:3 -value value2
```

After the data is prepared, the join can be executed:

```bash
$ $BBOXDB_HOME/bin/cli.sh -action join -table mydgroup_table1:mydgroup_table2 -bbox 0:20:0:20
Executing join query...
===============
Joined bounding box: [1.0:2.0,1.0:2.0]

Table: mydgroup_table1
Tuple: Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value1, version timestamp=1517232010086000

Table: mydgroup_table2
Tuple: Key key1, BoundingBox=[1.0:20.0,1.0:20.0], value=value1, version timestamp=1517232044346000
===============

===============
Joined bounding box: [1.0:2.0,1.0:2.0]

Table: mydgroup_table1
Tuple: Key key1, BoundingBox=[1.0:2.0,1.0:2.0], value=value1, version timestamp=1517232010086000

Table: mydgroup_table2
Tuple: Key key2, BoundingBox=[1.0:3.0,1.0:3.0], value=value2, version timestamp=1517232048595000
===============

===============
Joined bounding box: [5.0:7.0,5.0:7.0]

Table: mydgroup_table1
Tuple: Key key2, BoundingBox=[5.0:7.0,5.0:7.0], value=value2, version timestamp=1517232030282000

Table: mydgroup_table2
Tuple: Key key1, BoundingBox=[1.0:20.0,1.0:20.0], value=value1, version timestamp=1517232044346000
===============

===============
Joined bounding box: [10.0:12.0,10.0:12.0]

Table: mydgroup_table1
Tuple: Key key3, BoundingBox=[10.0:12.0,10.0:12.0], value=value3, version timestamp=1517232033667000

Table: mydgroup_table2
Tuple: Key key1, BoundingBox=[1.0:20.0,1.0:20.0], value=value1, version timestamp=1517232044346000
===============

Join done
``` 

# What's Next

* Visit our [website](http://bboxdb.org)
* Read the [documentation](http://jnidzwetzki.github.io/bboxdb/).
* Integrate the _BBoxDB Client_ into your own applications. You find an example and some documentation [here](http://jnidzwetzki.github.io/bboxdb/doc/client.html).
* Follow us on [Twitter](https://twitter.com/bboxdb) to stay informed.
* Read the [source code](https://github.com/jnidzwetzki/bboxdb/).
* Report Bugs [here](https://github.com/jnidzwetzki/bboxdb/issues).
* Submit patches.
