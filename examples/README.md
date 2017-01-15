# Info
This directory contains some examples for the BBoxDB. In addition, this document covers the tools to benchmark the software. To work with the examples and the benchmarks, the BBoxDB has to be started first. This is shown in the next section.

# Examples
The "client example" (BBoxDBClientExample.java) shows how data can be inserted and queried.

## Build and execute the client example

	# Build the example
	javac -classpath "../target/*":"../target/lib/*":"../conf":"." BBoxDBClientExample.java
	# Execute the example
	java -classpath "../target/*":"../target/lib/*":"../conf":"." BBoxDBClientExample
	
# Benchmarks

## The insert benchmark
	
	# Execute the benchmark
	java -classpath "../target/*":"../target/lib/*":"../conf" org.bboxdb.performance.BenchmarkInsertPerformance

	# Plot the query result
	# (the grep calls are removing the logger statements and let only the performance data pass)
	java -classpath "../target/*":"../target/lib/*":"../conf" org.bboxdb.performance.BenchmarkInsertPerformance | grep -v '\[' | grep -v Done > graph/InsertPerformance.dat
	# Let gnuplot plot the performance data
	gnuplot InsertPerformance.plot 
	
## The key query benchmark

	# Execute the benchmark
	java -classpath "../target/*":"../target/lib/*":"../conf" org.bboxdb.performance.BenchmarkKeyQueryPerformance

## The OSM insert benchmark

	# Execute the benchmark
	java -classpath "../target/*":"../target/lib/*":"../conf" org.bboxdb.performance.BenchmarkOSMInsertPerformance <OSM file> <type> <replication factor>
	
	# For example
	java -classpath "../target/*":"../target/lib/*":"../conf" org.bboxdb.performance.BenchmarkOSMInsertPerformance /tmp/hamburg-latest.osm.pbf roads 1
	