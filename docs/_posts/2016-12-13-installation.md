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

### Clock synchronization
BBoxDB uses timestamps to order opertions. So, it is useful to have a reliable time source on all systems. We strongly recommend to synchronize the local clock with an NTP server. You can accomplish this by executing:

```bash
apt-get install ntp
``` 

## Setup the environment
Let the environment variable BBOXDB_HOME point to the installation directory. For example ```/opt/bboxdb```:

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
The following sections are covering the initial setup of the BBoxDB.

## Nodes of the system
BBoxDB employs [Apache Zookeeper](https://zookeeper.apache.org/) to coordinate the distributed system. Thefore, a BBoxDB installation consists of two different node types: BBoxDB nodes and Zookeeper nodes. You have to specify the names of the BBoxDB nodes in the file ```$BBOXDB_HOME/conf/bboxdb-nodes``` and the names of the Zookeeper nodes in the file ```$BBOXDB_HOME/conf/zookeeper-nodes```:

```bash
vi $BBOXDB_HOME/conf/zookeeper-nodes
vi $BBOXDB_HOME/conf/bboxdb-nodes
```

## Ports and JVM parameter
The file `$BBOXDB_HOME/misc/bboxdb-env.sh` contains the configuration options for the BBoxDB service and the JVM parameter. You should open the file and adjust the settings to your preferences. 

__Notice:__ At least, the parameter `jmx_password` should be customized. Otherwise unauthorized attackers could connect to the JMX interface of the JVM and perform operations like a shutdown of the BBoxDB.

```bash
vi $BBOXDB_HOME/misc/bboxdb-env.sh
```

## Starting and Stopping the system
To manage the processes of BBoxDB, two scripts are provided: ```$BBOXDB_HOME/misc/manage_instance.sh`` is used to start and stop the processes on the local node. ```$BBOXDB_HOME/misc/manage_cluster.sh``` is used to managed a whole cluster of nodes.

Both scripts accept the following parameter. Depending on the script, the task is performed on the local system or on the whole cluster.

|    Parameter       |            Description            |
|--------------------|-----------------------------------|
| zookeeper_start    | Configure and start zookeeper     |
| zookeeper_stop     | Stop zookeeper                    |
| zookeeper_drop     | Stop zookeeper and drop the stored data |
| zookeeper_client   | Connect to the local zookeeper process and open an interactive shell. __This parameter is only available with the manage_instance.sh script__ |

