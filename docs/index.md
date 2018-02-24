---
layout: default
title: "Documentation of the BBoxDB"
---

<img src="logo/logo.png" width="400">

# Documentation

Welcome to the documentation of BBoxDB. BBoxDB is a research project to evaluate a novel database architecture for multi-dimensional big data. BBoxDB is designed as a distributed system; new nodes can be added to process larger amounts of data.

In contrast to traditional key-value stores, BBoxDB is optimized to handle multi-dimensional data. Stored data is placed into an n-dimensional space and parts of the space are handled by different nodes. SSTables (string sorted tables) are used as data storage, which provides a fast read and write access. [Apache Zookeeper](https://zookeeper.apache.org/) is used to coordinate the whole system. The system can be accessed, using a [network protocol](/bboxdb/dev/network.html). Some special features like continuous queries or a history for tuples are also supported.

Tools like a _graphical user interface_ (GUI) and a _command line interface_ (CLI) are available to work with the system. In addition, a [client](/bboxdb/doc/client.html) for the Java programming language is also available in the [maven central repository](https://search.maven.org/#search%7Cga%7C1%7Cbboxdb).