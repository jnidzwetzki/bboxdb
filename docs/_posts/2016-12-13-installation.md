---
layout: page
title: "Install & Setup"
category: doc
date: 2016-12-12 22:46:12
order: 1
---

# Building from Source
The software is tested on Debian and Ubuntu Linux.

## Dependencies 
BBoxDB depends on the following programs:

- Java (>= 8)
- Git
- Maven

On Debian and Ubuntu, you can install them by typing:

```bash
apt-get install openjdk-8-jdk ant maven git
```

## Setup the environment
Let the environment variable BBOXDB_HOME point to the installation directory. For example /opt/bboxdb:

```bash
export BBOXDB_HOME=/opt/bboxdb
echo 'export BBOXDB_HOME=/opt/bboxdb' >> .bashrc
```

## Downloading the Software
```bash
git clone https://github.com/jnidzwetzki/bboxdb
mv bboxdb $BBOXDB_HOME
```

# Initial Setup
BBoxDB employs [Apache Zookeeper](https://zookeeper.apache.org/) to coordinate the distributed system. Thefore, a BBoxDB installation consists of two different node types: BBoxDB nodes and Zookeeper nodes. You have to specify the names of the nodes in the files ```$BBOXDB_HOME/conf/bboxdb-nodes``` and ```$BBOXDB_HOME/conf/zookeeper-nodes```. You can do this by editing the two files:

```bash
vi $BBOXDB_HOME/conf/zookeeper-nodes
vi $BBOXDB_HOME/conf/bboxdb-nodes
```


