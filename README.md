<img src="docs/logo/logo.png" width="400">

# What is BBoxDB?

BBoxDB is a highly available distributed storage manager, designed to handle multi-dimensional big data. BBoxDB uses SSTables (String Sorted Tables) for a high throughput of read and write operations. Primarily, the software is a research project to explore new ways to handle multi-dimensional data in a distributed environment.

<a href="https://travis-ci.org/jnidzwetzki/bboxdb">
  <img alt="Build Status" src="https://travis-ci.org/jnidzwetzki/bboxdb.svg?branch=master">
</a> <a href="https://scan.coverity.com/projects/jnidzwetzki-bboxdb">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/11479/badge.svg"/>
</a> <a href="https://codecov.io/gh/jnidzwetzki/bboxdb">
  <img src="https://codecov.io/gh/jnidzwetzki/bboxdb/branch/master/graph/badge.svg" alt="Codecov" />
</a> <a href="https://gitter.im/bboxdb/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge">
  <img alt="Join the chat at https://gitter.im/bboxdb/Lobby" src="https://badges.gitter.im/Join%20Chat.svg">
  </a> <a href="https://repo1.maven.org/maven2/org/bboxdb/"><img alt="Maven Central Version" src="https://maven-badges.herokuapp.com/maven-central/org.bboxdb/bboxdb-server/badge.svg" />
  </a> <a href="https://codeclimate.com/github/jnidzwetzki/bboxdb/maintainability"><img src="https://api.codeclimate.com/v1/badges/0b8b98bde4ec65bfb5b7/maintainability" /></a><a href="http://makeapullrequest.com">
 <img src="https://img.shields.io/badge/PRs-welcome-brightgreen.svg" />
</a>

## What is the difference to traditional key-value-stores?

[NoSQL databases](https://en.wikipedia.org/wiki/NoSQL) and especially [key-value stores](https://en.wikipedia.org/wiki/Key-value_database) are very popular these days. They have a simple data model and can be easily implemented as a distributed system to handle big data. Techniques like _hash-_ or _range-partitioning_ are used to spread the data across a cluster of nodes. Each node stores only a small part of the whole dataset. 

Key-value stores are using the key to retrieve the data. When the key is known, the data can be accessed easily. When the key for a value is not known, a complete scan of the data has to be carried out. Each value has to be loaded and tested if it matches the search criteria. This is a very expensive operation that should be avoided in any case. 

Using the key to locate the data works well with one-dimensional data, but it becomes problematic when multi-dimensional data has to be stored. Its hard to find a proper key for a multi-dimensional value. 

<img src="docs/images/key_example.jpg" width="400">

_Example with one-dimensional data:_ The records of your customers are stored using the _customer id_ as key in a key-value store. When the customer logs in to your online shop with his customer id and his password, you can simply retrieve the customer record from the key-value store.

_Example with two-dimensional data:_ When you store the geographical information about a road, which key should you choose for the record? You can use the name of the road (e.g., _road 66_). However, this type of key does not help you to retrieve the road by knowing its coordinates. If you have a certain point in space and want to know which roads are located there, you have to perform a full data scan. 

It is almost impossible to find a suitable key that allows locating multi-dimensional data efficiently. Existing techniques like linearization using a [Z-Curve](https://en.wikipedia.org/wiki/Z-order_curve) could encode multiple-dimensions into a one-dimensional key. Point data can be handled, but data with an extent (e.g., a road) is hard to handle. This is the reason why BBoxDB was developed.

BBoxDB extends the simple key-value data model; each value is stored together with a key and a bounding box. This is the reason why BBoxDB is called a _key-bounding-box-value store_. The bounding box describes the location of the data in an n-dimensional space. A space partitioner (a [KD-Tree](https://en.wikipedia.org/wiki/K-d_tree), a [Quad-Tree](https://en.wikipedia.org/wiki/Quadtree) or a [Grid](https://en.wikipedia.org/wiki/Grid_file)) is responsible to partition the whole space into partitions (_distribution regions_) and assign these regions to nodes in the cluster. Depending on the used space partitioner, the regions are split and merged dynamically (_scale up_ and _scale down_), according to the amount of stored data. Point data and data which an extent (regions) of any dimension are supported by BBoxDB.

Hyperrectangle queries (range queries) can be efficiently handled by BBoxDB. This type of query is backed by a two-level index structure. Besides, BBoxDB can store tables with the same dimensions co-partitioned, this means that the data of the same regions in space are stored on the same nodes. Equi- and [spatial-joins](http://wiki.gis.com/wiki/index.php/Spatial_Join) can be processed efficiently locally on co-partitioned data. No data shuffling is needed during query processing.

BBoxDB is implemented as a distributed and highly available system. It supports the _available_ and _partiotion tollerance_ aspects of the [CAP theorem](https://en.wikipedia.org/wiki/CAP_theorem). [SSTables](https://research.google.com/archive/bigtable.html) (string sorted tables) are used as data storage. [Apache Zookeeper](https://zookeeper.apache.org/) is used to coordinate the whole system. Some special features like continuous queries or a history for tuples are also supported.

## Documentation 
The documentation of the project is located at [http://jnidzwetzki.github.io/bboxdb/](http://jnidzwetzki.github.io/bboxdb/). The documentation also contains the [changelog](http://jnidzwetzki.github.io/bboxdb/dev/changelog.html) of the project.

## Getting started
See the [getting started](http://jnidzwetzki.github.io/bboxdb/doc/gettingstarted.html) chapter in the documentation. We also recommend to read the examples located in the [bboxdb-examples/src/main/java](bboxdb-examples/src/main/java/) directory.

## Screenshots
BBoxDB ships with a GUI that allows observing the global index structure. Below you find two screenshots of the GUI. The screenshots show how the space is partitioned. In addition, some details about the discovered nodes are shown. 

<p><img src="docs/images/bboxdb_gui1.jpg" width="400"> <img src="docs/images/bboxdb_gui2.jpg" width="400"><br>
(The screenshot contains content from <a href="https://www.openstreetmap.org/">OpenSteetMap</a> - CC-BY-SA 2.0)
</p>

When 2-dimensional bounding boxes with [WGS 84](https://de.wikipedia.org/wiki/World_Geodetic_System_1984) coordinates are used, a map overlay visualization is supported by the GUI. On the right picture, some spatial data about Germany was imported and the Figure shows, how Germany in partitioned after the data was imported.


## Contact / Stay informed
* Visit our [website](http://bboxdb.org)
* Join our chat at [gitter](https://gitter.im/bboxdb/Lobby)
* Follow us on Twitter: [@BBoxDB](https://twitter.com/BBoxDB)
* Subscribe our mailing list at [Google Groups](https://groups.google.com/forum/#!forum/bboxdb)
* Visit our [bug tracking system](https://github.com/jnidzwetzki/bboxdb/issues)
* Read the [source code](https://github.com/jnidzwetzki/bboxdb) and the [documentation](http://jnidzwetzki.github.io/bboxdb/)
* For contributing, see our [contributing guide](https://github.com/jnidzwetzki/bboxdb/blob/master/CONTRIBUTING.md)
* If you like the project, please star it on GitHub!

## License
BBoxDB is licensed under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
