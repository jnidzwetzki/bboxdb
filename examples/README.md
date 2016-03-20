# How to build an execute the examples

## Start a least one scalephant instance
    ./misc/manage_instance.sh zookeeper_start
    ./misc/manage_instance.sh scalephant_start
    
## Build and execute the client example

	# Build the example
	javac -classpath "../target/*":"../target/lib/*":"../conf":"." ScalephantClientExample.java
	# Execute the example
	java -classpath "../target/*":"../target/lib/*":"../conf":"." ScalephantClientExample
	
