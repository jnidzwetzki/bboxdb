---
layout: page
title: "Zookeeper"
category: dev
date: 2016-12-12 12:18:27
---

# Zookeeper directory layout

## Group membership, service discovery and information
Zookeeper is used for group membership and service discovery. An entry the in zookeeper directory is used for both purposes. Each running instance of the BBoxDB creates a ephemeral node. These nodes are automatically removed, when the instance stops or crashes.

	/clustername/nodes/active/instancename

* clustername - is a freely selectable name for the cluster.
* node/active - the directory for active group memberships.
* instancename - the name of the running BBoxDB instance in the format ip:port. The value of the node is the state of the node. Allowed values are 'readonly' and 'readwrite'.

For example: /mycluster/nodes/active/192.168.1.1:5050[readonly]

	/clustername/nodes/version/instancename
	
* clustername - is a freely selectable name for the cluster
* node/version - the directory for active group memberships
* instancename - the name of the running BBoxDB instance in the format ip:port. The value of the node is the version of the software running on the system.

For example: /mycluster/nodes/version/192.168.1.1:5050[0.1.0]

## Distribution Groups
Distribution Groups specify, which part of the data is stored on which node. 

	/clustername/distribution/distributiongroupname

* clustername - is a freely selectable name for the cluster
* distribution - the directory for data distribution
* distributiongroupname - the name of the distribution group (e.g. 2_mygroup - for a 2 dimensional distribution group with the name mygroup) 

The nodes are created as sequential persistent nodes. The node with the lowest sequence number is used as the coordinator for write requests. A child with the name 'version' is created for each member of the distribution group. This version is used by the nodes, to deal with outdated nodes and to resync data if needed.

For example, a non split distribution group:

    /clustername/distribution/1_mygroup/nameprefixqueue/id-0
    /clustername/distribution/1_mygroup/nameprefix[0]
    /clustername/distribution/1_mygroup/replication[3]
	/clustername/distribution/1_mygroup/systems/192.168.1.1:5050[12345]
	/clustername/distribution/1_mygroup/systems/192.168.1.2:5050[12345]

When a 1 dimensional space is spited at point 50, the directory structure changes as follows:

    /clustername/distribution/1_mygroup/nameprefixqueue/id-0
    /clustername/distribution/1_mygroup/nameprefixqueue/id-1
    /clustername/distribution/1_mygroup/nameprefixqueue/id-2
    /clustername/distribution/1_mygroup/nameprefix[0]
    /clustername/distribution/1_mygroup/replication[3]
	/clustername/distribution/1_mygroup/split[50]
	/clustername/distribution/1_mygroup/left/nameprefix[1]
	/clustername/distribution/1_mygroup/left/systems/192.168.1.1:5050[12345]
	/clustername/distribution/1_mygroup/left/systems/192.168.1.2:5050[12345]
	/clustername/distribution/1_mygroup/right/nameprefix[2]
	/clustername/distribution/1_mygroup/right/systems/192.168.1.2:5050[12347]
	/clustername/distribution/1_mygroup/right/systems/192.168.1.1:5050[12347]
	
The instance '192.168.1.1:5050' is the write coordinator for the interval [\*, 50], the instance '192.168.1.2:5050' is the write coordinator for the interval (50, \*].

# Initial start
Upon the first initialization of a BBoxDB instance, only creates the Zookeeper entry for group membership and service discovery. Because the node does not store date, recovery or registration in a distribution group is not necessary.

# Creating a new distribution group
When a client creates the first table in a distribution group, the distribution group needs to be created first. Two situations are possible in this moment: (i) unused BBoxDB instances are available and (ii) all instances already store some data. In the first situation, the client pinks randomly a BBoxDB instance as the write coordinator for the whole distribution group. In the second situation, the client pinks one BBoxDB instance randomly. 

The same algorithm is used, when the placements for the replicates are determined. 

# Normal system start
Upon the normal system start, the BBoxDB instance read all entries from the distribution group and verify that the local version and the highest version number in zookeeper for that part of the distribution group matches. If not, he queries the node with the highest version number and asks of the missing tuples.

# Distribution region states
* <tt>creating</tt> - The distribution region is created but not ready. Some tasks like replica placement are currently running.
* <tt>active</tt> - The distribution region is ready. Data can be read and written. If the parent node is in state 'splitting', the parent has also to be contacted for read operations.
* <tt>splitting</tt> - The distribution region is splitting. Existing data is spread to the child regions. Data can still be read. New data has to be written to the child regions. 
* <tt>split</tt> - All data is split. No read or write requests are possible in this distribution group.

