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
	