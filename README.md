<img src="docs/logo/logo.png" width="400"> <br>

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
 <a href="https://hub.docker.com/r/jnidzwetzki/bboxdb/"><img src="https://img.shields.io/docker/stars/jnidzwetzki/bboxdb.svg">
 </a>

__Please Note:__ The master branch may be in an unstable state during development. Please use our releases for productive environments.

# What is BBoxDB?
BBoxDB is a highly available distributed storage manager, designed to handle multi-dimensional big data.  Primarily, the software is a research project to explore new ways to handle multi-dimensional data in a distributed environment. 

In contrast to existing key-value stores, BBoxDB can handle multi-dimensional efficiently. Existing key-value stores are using one-dimensional keys to address the values. Finding a proper key for multi-dimensional data is hard and often impossible; this is especially true when the data has an extent (e.g., non-point data / regions). To retrieve multi-dimensional data from a key-value store, a full data scan is often required. BBoxDB was developed to avoid the expensive full data scan and to make the work with multi-dimensional data more convenient.

# BBoxDB in Action
In the following screencast, the _command line interface_ of BBoxDB is used to create a 2-dimensional distribution group and two tables. Then some tuples are inserted and operations like key-queries, hyperrectangle-queries, deletes, and joins are executed on the stored data. For accessing BBoxDB from your application, see the [creating client code](https://jnidzwetzki.github.io/bboxdb/doc/client.html) section in the documentation.

<p><a href="https://github.com/jnidzwetzki/bboxdb/blob/master/docs/images/screencast.gif?raw=true" target="_blank"><img src="docs/images/screencast.gif" ></a><br><i>If the font is difficult to read, please click on the image.</i></p>

## Documentation 
The documentation of the project is located at [https://jnidzwetzki.github.io/bboxdb/](https://jnidzwetzki.github.io/bboxdb/). The documentation also contains the [changelog](http://jnidzwetzki.github.io/bboxdb/dev/changelog.html) of the project.

## Getting started
For a guided tour through the features of BBoxDB, see the [getting started](https://jnidzwetzki.github.io/bboxdb/doc/gettingstarted.html) chapter in the documentation. We also recommend reading the [creating client code section](https://jnidzwetzki.github.io/bboxdb/doc/client.html). The [install guide](https://jnidzwetzki.github.io/bboxdb/doc/installation.html) explains the needed steps to deploy an own BBoxDB cluster. The guide also describes how you can setup a virtualized cluster with 5 BBoxDB nodes in under two minutes, by using [Docker](https://hub.docker.com/r/jnidzwetzki/bboxdb/) and [Docker Compose](https://docs.docker.com/compose/).

## Screenshots
BBoxDB ships with a GUI that allows observing the global index structure. Below you find two screenshots of the GUI. The screenshots show how the space is partitioned. In addition, some details about the discovered nodes are shown. 

<p><img src="docs/images/bboxdb_gui1.jpg" width="400"> <img src="docs/images/bboxdb_gui2.jpg" width="400"><br>
<i>(The screenshot contains content from <a href="https://www.openstreetmap.org/">OpenSteetMap</a> - CC-BY-SA 2.0)</i>
</p>

When 2-dimensional bounding boxes with [WGS 84](https://de.wikipedia.org/wiki/World_Geodetic_System_1984) coordinates are used, a map overlay visualization is supported by the GUI. On the right picture, some spatial data about Germany was imported and the Figure shows, how Germany in partitioned after the data was imported.


## Contact / Stay informed
* Visit our [website](https://bboxdb.org)
* Read our [technical report](https://ub-deposit.fernuni-hagen.de/receive/mir_mods_00001277)
* Join our chat at [gitter](https://gitter.im/bboxdb/Lobby)
* Follow us on Twitter: [@BBoxDB](https://twitter.com/BBoxDB)
* Subscribe our mailing list at [Google Groups](https://groups.google.com/forum/#!forum/bboxdb)
* Visit our [bug tracking system](https://github.com/jnidzwetzki/bboxdb/issues)
* Read the [source code](https://github.com/jnidzwetzki/bboxdb) and the [documentation](https://jnidzwetzki.github.io/bboxdb/)
* For contributing, see our [contributing guide](https://github.com/jnidzwetzki/bboxdb/blob/master/CONTRIBUTING.md)
* If you like the project, please star it on GitHub!

## License
BBoxDB is licensed under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
