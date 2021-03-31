<img src="docs/logo/logo.png" width="400"> <br>

<a href="https://scan.coverity.com/projects/jnidzwetzki-bboxdb">
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
 <a href="https://hub.docker.com/r/jnidzwetzki/bboxdb/"><img src="https://img.shields.io/docker/stars/jnidzwetzki/bboxdb.svg">
 </a>

__Please Note:__ The master branch may be in an unstable state during development. Please use our releases for productive environments.

# What is BBoxDB?
BBoxDB is a highly available distributed storage manager designed to handle multi-dimensional big data. In contrast to existing key-value stores, BBoxDB can handle multi-dimensional efficiently. Existing key-value stores are using one-dimensional keys to address the values. Finding a proper key for multi-dimensional data is challenging and often impossible; this is especially true when the data has an extent (non-point data / regions). To retrieve multi-dimensional data from a key-value store, a full data scan is often required. BBoxDB was developed to avoid the expensive full data scan and to make the work with multi-dimensional data more convenient. User-defined filters are supported to process custom data formats, and BBoxDB also supports the handling of data streams.

<p><img src="docs/images/space.jpg" width="400"></p>

## Key features
* ✅ A distributed and fault-tolerant data store for n-dimensional data.
* ✅ Data (point and non-point) of any dimension is supported.
* ✅ The data is indexed, which enables efficient range query processing.
* ✅ BigData is supported by spreading the data across a cluster of systems. Each node stores only a small part of the whole dataset.
* ✅ Multi-dimensional shards are created dynamically on the actual distribution of the data (automatically scale-up/scale-down).
* ✅ Data of multiple tables is stored co-partitioned, and spatial-joins can be executed efficiently without data shuffling between nodes.
* ✅ Data are re-distributed in the background without any service interruption.
* ✅ Continuous queries with bounding box query predicates are supported.
* ✅ User-defined filters for query processing on custom data types. 

## Documentation
The documentation of the project is located at [https://jnidzwetzki.github.io/bboxdb/](https://jnidzwetzki.github.io/bboxdb/). The documentation also contains the [changelog](http://jnidzwetzki.github.io/bboxdb/dev/changelog.html) of the project.

## Getting started
For a guided tour through the features of BBoxDB, see the [getting started](https://jnidzwetzki.github.io/bboxdb/doc/gettingstarted.html) chapter in the documentation. We also recommend reading the [creating client code section](https://jnidzwetzki.github.io/bboxdb/doc/client.html). The [install guide](https://jnidzwetzki.github.io/bboxdb/doc/installation.html) explains the needed steps to deploy an own BBoxDB cluster. The guide also describes how you can setup a virtualized cluster with 5 BBoxDB nodes in under two minutes, by using [Docker](https://hub.docker.com/r/jnidzwetzki/bboxdb/) and [Docker Compose](https://docs.docker.com/compose/).

## Screenshots
BBoxDB ships with a GUI that allows observing the global index structure. Below you find two screenshots of the GUI. The screenshots show how the space is partitioned. In addition, some details about the discovered nodes are shown. When two-dimensional bounding boxes with [WGS 84](https://de.wikipedia.org/wiki/World_Geodetic_System_1984) coordinates are used, a map overlay visualization is supported by the GUI. On the top right picture, some spatial data about Germany was imported and the Figure shows, how Germany in partitioned after the data was imported. In addition, the GUI provides operations to explore two dimensional GeoJSON encoded data.

<p><img src="docs/images/bboxdb_gui1.jpg" width="400"> <img src="docs/images/bboxdb_gui2.jpg" width="400"><br>
<br>
<p><img src="docs/images/bboxdb_gui3.jpg" width="400"> <img src="docs/images/bboxdb_gui4.jpg" width="400"><br>
<i>(The screenshots contain content from <a href="https://www.openstreetmap.org/">OpenStreetMap</a> - CC-BY-SA 2.0)</i>
</p>

BBoxDB is also able to handle data streams. The first screenshot shows the busses in Sydney fetched from a real-time GTFS feed. The data is provided by the [Transport for New South Wales Website](https://opendata.transport.nsw.gov.au/). The second screenshot shows the aircraft traffic in the area of Berlin. The data is fetched from the Automatic Dependent Surveillance–Broadcast (ADS–B) data feed from the [ADSBHub Website](https://www.adsbhub.org/). For more details about that, see our [tutorial](https://jnidzwetzki.github.io/bboxdb/doc/tutorial-streamdata.html) on the handling of real-world data streams.

<p><img src="docs/images/bboxdb_sydney.jpg" width="400"> <img src="docs/images/bboxdb_adsb.jpg" width="400"><br>
<i>(The screenshots contain content from <a href="https://www.openstreetmap.org/">OpenStreetMap</a> - CC-BY-SA 2.0)</i>
</p>

## Contact / Stay informed
* Visit our [website](https://bboxdb.org)
* Read our research papers:
  * [Technical report](https://ub-deposit.fernuni-hagen.de/receive/mir_mods_00001277) about the basic ideas of the software.
  * [Demo paper](https://dl.acm.org/citation.cfm?id=3269208) about partitioning (presented at [CIKM 2018](https://www.cikm2018.units.it/)).
  * [Full paper](https://link.springer.com/article/10.1007/s10619-019-07275-w) at Springer Distributed and Parallel Databases. 
  * [Demo paper](https://ieeexplore.ieee.org/document/9005999) about user defined filters (presented at [IEEE Big Spatial Data 2019](http://cse.ucdenver.edu/~BSD2019/); received the best paper award).
  * [Demo paper](https://edbt2021proceedings.github.io/docs/p170.pdf) about BBoxDB Streams (presented at [EDBT 2021](https://edbticdt2021.cs.ucy.ac.cy)).
* Join our chat at [gitter](https://gitter.im/bboxdb/Lobby)
* Follow us on Twitter: [@BBoxDB](https://twitter.com/BBoxDB)
* Subscribe our mailing list at [Google Groups](https://groups.google.com/forum/#!forum/bboxdb)
* Visit our [bug tracking system](https://github.com/jnidzwetzki/bboxdb/issues)
* Read the [source code](https://github.com/jnidzwetzki/bboxdb) and the [documentation](https://jnidzwetzki.github.io/bboxdb/)
* For contributing, see our [contributing guide](https://github.com/jnidzwetzki/bboxdb/blob/master/CONTRIBUTING.md)
* If you like the project, please star it on GitHub!

## License
BBoxDB is licensed under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
