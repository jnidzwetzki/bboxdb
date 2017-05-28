# Info
This directory contains some examples for the BBoxDB. In addition, this document covers the tools to benchmark the software. 

# Examples
The "client example" (BBoxDBClientExample.java) shows how data can be inserted and queried.

## Build and execute the client example

	# Build the example
	javac -classpath "../target/*":"../target/lib/*":"../conf":"." BBoxDBClientExample.java
	# Execute the example
	java -classpath "../target/*":"../target/lib/*":"../conf":"." BBoxDBClientExample
