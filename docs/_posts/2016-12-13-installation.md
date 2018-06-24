---
layout: page
title: "Installation"
category: doc
date: 2016-12-12 22:46:12
order: 1
---
# Quickstart with Docker
Docker is the de-facto standard for packaging distributed applications. For easy deployments, we provide a Docker container [image](https://hub.docker.com/r/jnidzwetzki/bboxdb/) of BBoxDB. We also offer a Docker compose [file](https://github.com/jnidzwetzki/bboxdb/blob/master/misc/docker/docker-compose.yml) which installs a cluster consisting of three Zookeeper nodes and five BBoxDB nodes within two minutes.

## Setup a sample cluster
To set up the sample cluster, please enter the following commands. Please note that you have do install [Docker](https://docs.docker.com/glossary/?term=Docker) and [Docker Compose](https://docs.docker.com/compose/) first.

```bash
$ wget https://github.com/jnidzwetzki/bboxdb/blob/master/misc/docker/docker-compose.yml
$ cd bboxdb
$ docker-compose -d -f docker-compose.yml up
```

After the command is completed, you should have 8 containers running:


```bash
$ # docker ps
CONTAINER ID        IMAGE                                  COMMAND                  CREATED             STATUS              PORTS                                                NAMES
18f2094d7e6f        jnidzwetzki/bboxdb:latest              "/bboxdb/misc/docker…"   4 minutes ago       Up 4 minutes        0.0.0.0:10081->10085/tcp, 0.0.0.0:50501->50505/tcp   docker_bboxdb1_1
3cae87bd4c83        jnidzwetzki/bboxdb:latest              "/bboxdb/misc/docker…"   4 minutes ago       Up 4 minutes        0.0.0.0:10082->10085/tcp, 0.0.0.0:50502->50505/tcp   docker_bboxdb2_1
6072d04f4f9f        jnidzwetzki/bboxdb:latest              "/bboxdb/misc/docker…"   4 minutes ago       Up 4 minutes        0.0.0.0:10083->10085/tcp, 0.0.0.0:50503->50505/tcp   docker_bboxdb3_1
09bf6a5491fe        jnidzwetzki/bboxdb:latest              "/bboxdb/misc/docker…"   4 minutes ago       Up 4 minutes        0.0.0.0:10084->10085/tcp, 0.0.0.0:50504->50505/tcp   docker_bboxdb4_1
cc2f8632e16f        jnidzwetzki/bboxdb:latest              "/bboxdb/misc/docker…"   4 minutes ago       Up 4 minutes        0.0.0.0:10085->10085/tcp, 0.0.0.0:50505->50505/tcp   docker_bboxdb5_1
9b78d1d6d8da        zookeeper                              "/docker-entrypoint.…"   9 minutes ago       Up 4 minutes        2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp           docker_zk1_1
acfb02de781f        zookeeper                              "/docker-entrypoint.…"   9 minutes ago       Up 4 minutes        2888/tcp, 3888/tcp, 0.0.0.0:2183->2181/tcp           docker_zk3_1
79e73e60883d        zookeeper                              "/docker-entrypoint.…"   9 minutes ago       Up 4 minutes        2888/tcp, 3888/tcp, 0.0.0.0:2182->2181/tcp           docker_zk2_1
```

You can now enter one of the BBoxDB containers and work with the CLI of BBoxDB. For example, you can list all discovered BBoxDB nodes.

```bash
$ docker exec -it docker_bboxdb5_1 bash
bash-4.4# /bboxdb/bin/cli.sh -action show_instances -host zk1:2181
Connecting to BBoxDB cluster... [Established]
Show all discovered BBoxDB instances

#######
DistributedInstance [ip=172.18.0.5, port=50505, version=0.7.0, cpuCores=4, memory=1.7 GB, state=READY, storages=1, freeSpace()=610.1 GB, totalSpace()=1.8 TB]
DistributedInstance [ip=172.18.0.6, port=50505, version=0.7.0, cpuCores=4, memory=1.7 GB, state=READY, storages=1, freeSpace()=610.1 GB, totalSpace()=1.8 TB]
DistributedInstance [ip=172.18.0.7, port=50505, version=0.7.0, cpuCores=4, memory=1.7 GB, state=READY, storages=1, freeSpace()=610.1 GB, totalSpace()=1.8 TB]
DistributedInstance [ip=172.18.0.8, port=50505, version=0.7.0, cpuCores=4, memory=1.7 GB, state=READY, storages=1, freeSpace()=610.1 GB, totalSpace()=1.8 TB]
DistributedInstance [ip=172.18.0.9, port=50505, version=0.7.0, cpuCores=4, memory=1.7 GB, state=READY, storages=1, freeSpace()=610.1 GB, totalSpace()=1.8 TB]
#######
```

When you are done with your work, you can stop and delete the BBoxDB cluster.

```bash
$ docker-compose -f docker-compose.yml down
$ docker-compose -f docker-compose.yml rm
```

# Manuall installation
## Dependencies 
BBoxDB depends on the following programs:

- Java (>= 8, Oracle)
- Git
- Maven

On Ubuntu, you can install them by typing:

```bash
apt-get install oracle-java8-installer ant maven git
```

On Debian, you can install them by typing:

```bash
apt-get install java-package ant maven git

# Download the JDK from Oracle, replace $version with the current version of the JVM
make-jpkg jdk-$version-linux-x64.tar.gz
dpkg -i oracle-java8-jdk_$version_amd64.deb
```

_Notice:_ You could also use OpenJDK, but we are recommending the Oracle JVM. BBoxDB uses memory mapped files, features such as cleaning allocated memory is not available in all JVMs. Also, we recommend using a 64-bit operating system. Due to the memory mapped files, BBoxDB needs a huge virtual address space. 32-bit systems can address up to 4 GB memory, which makes it hard to handle big datasets.

_Notice:_ BBoxDB should run on every Linux distribution, but at the moment the software is only tested on Debian and Ubuntu Linux.

## Clock synchronization
BBoxDB uses timestamps to order operations. So, it is useful to have a reliable time source on all systems. We strongly recommend to synchronize the local clock with an NTP server. You can accomplish this by executing:

```bash
apt-get install ntp
``` 

## Setup the environment
Let the environment variable BBOXDB_HOME point to the installation directory. For example ```/opt/bboxdb```:

```bash
export BBOXDB_HOME=/opt/bboxdb
echo 'export BBOXDB_HOME=/opt/bboxdb' >> .bashrc
```

## Downloading and compiling the software
```bash
git clone https://github.com/jnidzwetzki/bboxdb
mv bboxdb $BBOXDB_HOME
$BBOXDB_HOME/bin/manage_instance.sh bboxdb_update
```

The last command executes maven, downloads all needed dependencies and complies BBoxDB. This step has to be repeated on every node in your cluster.

## Installing BBoxDB
The following sections are covering the initial setup of the BBoxDB.

### Setup the nodes of the system
BBoxDB employs [Apache Zookeeper](https://zookeeper.apache.org/) to coordinate the distributed system. BBoxDB includes some helper scripts to install and configure the Zookeeper installation automatically. To setup the Zookeeper and BBoxDB nodes, you only need to define the name of the hosts in two files. The file ```$BBOXDB_HOME/conf/bboxdb-nodes``` contains the name of all BBoxDB nodes and the file ```$BBOXDB_HOME/conf/zookeeper-nodes``` contains the names of all Zookeeper nodes. It is recommended to define at least 3 Zookeeper nodes. Bellow, you find an example with 3 Zookeeper nodes and 6 BBoxDB nodes. Zookeeper and BBoxDB can be installed on the same hardware.

```bash
cat $BBOXDB_HOME/conf/zookeeper-nodes
node1
node2
node3

cat $BBOXDB_HOME/conf/bboxdb-nodes
node1
node2
node3
node4
node5
node6
```

### Ports and JVM parameter
The file `$BBOXDB_HOME/bin/bboxdb-env.sh` contains the configuration options for the BBoxDB service and the JVM parameter. You should open the file and adjust the settings to your preferences. 

__Notice:__ At least, the parameter `jmx_password` should be customized. Otherwise, unauthorized attackers could connect to the JMX interface of the JVM and perform operations like a shutdown of the BBoxDB.

```bash
vi $BBOXDB_HOME/bin/bboxdb-env.sh
```

### Starting and stopping the system
To manage the processes of BBoxDB, two scripts are provided: ```$BBOXDB_HOME/bin/manage_instance.sh``` is used to start and stop the processes on the local node. ```$BBOXDB_HOME/bin/manage_cluster.sh``` is used to managed a whole cluster of nodes.

Both scripts require one parameter. This parameter determines the operation of the script. Depending on the script, the task is performed only on the local system or on the whole cluster.

|    Parameter       |                Description                |
|--------------------|-------------------------------------------|
| zookeeper_start    | Configure and start zookeeper             |
| zookeeper_stop     | Stop zookeeper                            |
| zookeeper_drop     | Stop zookeeper and drop the stored data   |
| zookeeper_client   | Connect to the local zookeeper process and open an interactive shell. _This parameter is only available with the manage_instance.sh script_  |
| bboxdb_start       | Start the BBoxDB process                  | 
| bboxdb_start_debug | Start the BBoxDB process in debug mode    | 
| bboxdb_start_trace | Start the BBoxDB process  in trace mode   | 
| bboxdb_stop        | Stop the BBoxDB process                   |
| bboxdb_upgrade     | Download the last version from github and compile the source |

__Example:__ Start a whole cluster:

```bash
$BBOXDB_HOME/bin/manage_cluster.sh zookeeper_start
$BBOXDB_HOME/bin/manage_cluster.sh bboxdb_start
```

__Example:__ Stop a whole cluster:


```bash
$BBOXDB_HOME/bin/manage_cluster.sh bboxdb_stop
$BBOXDB_HOME/bin/manage_cluster.sh zookeeper_stop
```

### Logfiles
Zookeeper and BBoxDB messages are written into logfiles. Depending on the start parameter of the server ```bboxdb_start```, ```bboxdb_start_debug``` or ```bboxdb_start_trace```, a different amount of details are written into the log files.

|    Logfile                  |            Content                |
|-----------------------------|-----------------------------------|
| $BBOXDB_HOME/zookeeper.log  | Details about the operation of Zookeeper     |
| $BBOXDB_HOME/bboxdb.log     | Details about the operation of BBoxDB        |

