# Zookeeper directory layout

## Group membership
The group membership determines, which instances of the scalephant are currently running faultless. The nodes are created as ephemeral nodes. They will be automatically removed, when the instance stops or crashes.

	/clustername/nodes/instancename

* clustername - is a freely seclectable name for the cluster
* node - the directory for group memberships
* instancename - the name of the running scalephant instance in the format ip:port

For example: /mycluster/nodes/192.168.1.1:5050

## Distribution Groups
Distribution Groups specify, which part of the data is stored on which node. 

	/clustername/distribution/distributiongroupname/root-id

* clustername - is a freely seclectable name for the cluster
* distribution - the directory for data distribution
* distributiongroupname - the name of the distribution group (e.g. 2_mygroup - for a 2 dimensional distribution group with the name mygroup) 

The nodes are created as sequencial persistent nodes. The node with the lowest sequence number is used as the coordinator for write requests.

For example, a non splitted distribution group:

	/clustername/distribution/1_mygroup/instances/0[192.168.1.1:5050]
	/clustername/distribution/1_mygroup/instances/1[192.168.1.2:5050]

When the 1 dimensional space is splitted at point 50, the directory structure changes as follows:

	/clustername/distribution/1_mygroup/instances
	/clustername/distribution/1_mygroup/split[50]
	/clustername/distribution/1_mygroup/left/instances/0[192.168.1.1:5050]
	/clustername/distribution/1_mygroup/left/instances/1[192.168.1.2:5050]
	/clustername/distribution/1_mygroup/right/instances/0[192.168.1.2:5050]
	/clustername/distribution/1_mygroup/right/instances/1[192.168.1.1:5050]
	
The instance '192.168.1.1:5050' is the write coordinator for the interval [*, 50], the instance '192.168.1.2:5050' is the write coordinator for the interval (50, *].