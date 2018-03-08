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
  </a> <a href="https://codeclimate.com/github/jnidzwetzki/bboxdb/maintainability"><img src="https://api.codeclimate.com/v1/badges/0b8b98bde4ec65bfb5b7/maintainability" /></a>

## What is the difference to traditional key-value-stores?
Key-value-stores are using the key to retrieve the data. This works well with one-dimensional data but it becomes problematic, when multi-dimensional data has to be stored. Its hard to find a proper key for a multi-dimensional value. For example, when you store the geographical information about a road, which key should you choose for the road? 

BBoxDB in contrast extension of the simple key-value data model. Each value is stored together with a key and a bounding box. The bounding box describes the location of the data in an n-dimensional space. Hyperrectangle queries (range queries) can be efficiently handled by BBoxDB. This type of query is backed by a two-level index structure. In addition, BBoxDB can store tables with the same dimensions co-partitioned, this means that the data of the same regions in space are stored on the same nodes. Equi- and spatial-joins can be processed efficiently locally on co-partitoned data. No data shuffeling is needed during query processing.

## Documentation 
The documentation of the project is located at [http://jnidzwetzki.github.io/bboxdb/](http://jnidzwetzki.github.io/bboxdb/). The documentation also contains the [changelog](http://jnidzwetzki.github.io/bboxdb/dev/changelog.html) of the project.

## Getting started
See the [getting started](http://jnidzwetzki.github.io/bboxdb/doc/started.html) chapter in the documentation. We also recommend to read the examples located in the [bboxdb-examples/src/main/java](bboxdb-examples/src/main/java/) directory.

## Screenshots
<p><img src="docs/images/bboxdb_gui1.jpg" width="400"> <img src="docs/images/bboxdb_gui2.jpg" width="400"><br>
(The screenshot contains content from <a href="https://www.openstreetmap.org/">OpenSteetMap</a> - CC-BY-SA 2.0)
</p>

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
