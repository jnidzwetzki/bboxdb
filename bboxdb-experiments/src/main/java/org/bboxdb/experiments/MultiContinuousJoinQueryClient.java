/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 *******************************************************************************/
package org.bboxdb.experiments;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.compress.archivers.zip.PKWareExtraHeader;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.QueryPlanBuilder;
import org.bboxdb.network.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.tools.helper.RandomQueryRangeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiContinuousJoinQueryClient implements Runnable {

	/**
	 * The cluster contact point
	 */
	private final String contactPoint;
	
	/**
	 * The name of the cluster
	 */
	private final String clusterName;
	
	/**
	 * The stream table to query
	 */
	private final String streamTable;

	/**
	 * The persistent table to query
	 */
	private final String persistentTable;
	
	/**
	 * The data ranges
	 */
	private final List<Hyperrectangle> ranges;
	
	/**
	 * All threads
	 */
	private final List<Thread> allThreads = new CopyOnWriteArrayList<>();
	
	/**
	 * The UDF name
	 */
	private final Optional<String> udfName;

	/**
	 * The UDF value
	 */
	private final Optional<String> udfValue;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MultiContinuousJoinQueryClient.class);


	public MultiContinuousJoinQueryClient(final String contactPoint, final String clusterName, 
			final String streamTable, final String persistentTable, final List<Hyperrectangle> ranges, 
			final Optional<String> udfName, 
			final Optional<String> udfValue) {
			
				this.contactPoint = contactPoint;
				this.clusterName = clusterName;
				this.streamTable = streamTable;
				this.persistentTable = persistentTable;
				this.ranges = ranges;
				this.udfName = udfName;
				this.udfValue = udfValue;
	}
	
	@Override
	public void run() {
		
		try(final BBoxDB connection = new BBoxDBCluster(contactPoint, clusterName)) {
			connection.connect();
			
			for(final Hyperrectangle queryRectangle : ranges) {
				System.out.println("Creating query in range: " + queryRectangle);
				
				final QueryPlanBuilder queryPlanBuilder = QueryPlanBuilder
						.createQueryOnTable(streamTable)
						.spatialJoinWithTable(persistentTable)
						.forAllNewTuplesInSpace(queryRectangle);
				
				if(udfName.isPresent()) {
					logger.info("Using UDF {} with value {}", udfName.get(), udfValue.get());
					final UserDefinedFilterDefinition udf = new UserDefinedFilterDefinition(udfName.get(), udfValue.get());
					queryPlanBuilder.addJoinFilter(udf);
				}
				
				final ContinuousQueryPlan queryPlan = queryPlanBuilder.build();
				
				final JoinedTupleListFuture queryFuture = connection.queryContinuous(queryPlan);
				readResultsInThread(queryFuture);
			}
			
			logger.info("Successfully registered {} queries", ranges.size());
			
			for(final Thread thread : allThreads) {
				thread.join();
			}
		} catch (BBoxDBException e) {
			logger.error("Got an exception", e);
		} catch (InterruptedException e) {
			return;
		} 
	}
	
	/**
	 * 
	 * @param queryFuture
	 */
	private void readResultsInThread(final JoinedTupleListFuture queryFuture) {
		final Thread thread = new Thread(() -> {
			
			try {
				queryFuture.waitForCompletion();
				
				if(queryFuture.isFailed()) {
					logger.error("Query error {}", queryFuture.getAllMessages());
					return;
				}
				
				for(final MultiTuple tuple : queryFuture) {
					tuple.getBoundingBox(); // Consume and ignore the tuple
				}
			} catch (InterruptedException e) {
				return;
			}
		});
		
		allThreads.add(thread);
		thread.start();
	}

	/**
	 * Main Main Main Main
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args.length < 1) {
			System.err.println("Usage: <Class> Action:{autogenerate, writefile, readfile}");
			System.exit(-1);
		}
		
		final String operation = args[0];
		
		if("autogenerate".equals(operation)) {
			performAutogenerate(args);
		} else if("writefile".equals(operation)) {
			performWriteData(args);
		} else if("readfile".equals(operation)) {
			performReadData(args);
		} else {
			System.err.println("Usage: <Class> Action:{autogenerate, writefile, readfile}");
			System.exit(-1);
		}
	}

	/**
	 * Perform the read data operation
	 * @param args
	 */
	private static void performReadData(final String[] args) {
		if(args.length != 6) {
			System.err.println("Usage: <Class> readfile <File> <ClusterContactPoint> <Clustername> <Stream-Table> <Persistent-Table>");
			System.exit(-1);
		}
		
		final File inputFile = new File(args[1]);
		final String contactPoint = args[2];
		final String clusterName = args[3];
		final String streamTable = args[4];
		final String persistetTable = args[5];

		
		Optional<String> udfName = Optional.empty();
		Optional<String> udfValue = Optional.empty();
		
		if(args.length == 8) {
			udfName = Optional.of(args[6]);
			udfValue = Optional.of(args[7]);
		}
		
		
		try (final Stream<String> lineStream = Files.lines(Paths.get(inputFile.getPath()))) {
	
			final List<Hyperrectangle> ranges = lineStream
					.map(s -> Hyperrectangle
					.fromString(s))
			 		.collect(Collectors.toList());
	
			final MultiContinuousJoinQueryClient runable = new MultiContinuousJoinQueryClient(contactPoint, clusterName, 
					streamTable, persistetTable, ranges, udfName, udfValue);
			
			runable.run();

		} catch (IOException e) {
			logger.error("Got exception during write", e);
		}
	}

	/**
	 * Write the queries into a file for later usage
	 * @param args
	 */
	private static void performWriteData(final String[] args) {
		if(args.length != 5) {
			System.err.println("Usage: <Class> writefile <File> <Range> <Percentage> <Parallel-Queries>");
			System.exit(-1);
		}

		final File outputFile = new File(args[1]);
		final String rangeString = args[2];
		final String percentageString = args[3];
		final String parallelQueriesString = args[4];
		
		if(outputFile.exists()) {
			System.err.println("The specified output file already exists");
			System.exit(-1);
		}
		
		final Optional<Hyperrectangle> range = HyperrectangleHelper.parseBBox(rangeString);
		
		if(! range.isPresent()) {
			System.err.println("Unable to parse as bounding box: " + rangeString);
		}
		
		final double percentage = MathUtil.tryParseDoubleOrExit(percentageString, () -> "Unable to parse: " + percentageString);
		final double parallelQueries = MathUtil.tryParseDoubleOrExit(parallelQueriesString, () -> "Unable to parse: " + parallelQueriesString);

		try(final BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
			for(int i = 0; i < parallelQueries; i++) {
				final Hyperrectangle queryRectangle = RandomQueryRangeGenerator.getRandomQueryRange(range.get(), percentage);
				writer.write(queryRectangle.toCompactString() + "\n");
			}
		} catch (IOException e) {
			logger.error("Got exception during write", e);
		}
	}

	/**
	 * Perform the autogenerate action
	 * @param args
	 */
	private static void performAutogenerate(final String[] args) {
		if(args.length != 8 && args.length != 10) {
			System.err.println("Usage: <Class> autogenerate <ClusterContactPoint> <Clustername> <Stream-Table> <Presistent-Table> <Range> "
					+ "<Percentage> <Parallel-Queries> {<UDF-Name> <UDF-Value>}");
			System.exit(-1);
		}
		
		final String contactPoint = args[1];
		final String clusterName = args[2];
		final String streamTable = args[3];
		final String persistentTable = args[4];
		final String rangeString = args[5];
		final String percentageString = args[6];
		final String parallelQueriesString = args[7];
		
		Optional<String> udfName = Optional.empty();
		Optional<String> udfValue = Optional.empty();
		
		if(args.length == 10) {
			udfName = Optional.of(args[8]);
			udfValue = Optional.of(args[9]);
		}
		
		
		final Optional<Hyperrectangle> range = HyperrectangleHelper.parseBBox(rangeString);
		
		if(! range.isPresent()) {
			System.err.println("Unable to parse as bounding box: " + rangeString);
		}
		
		final double percentage = MathUtil.tryParseDoubleOrExit(percentageString, () -> "Unable to parse: " + percentageString);
		final double parallelQueries = MathUtil.tryParseDoubleOrExit(parallelQueriesString, () -> "Unable to parse: " + parallelQueriesString);
		
		final List<Hyperrectangle> ranges = new ArrayList<>();

		for(int i = 0; i < parallelQueries; i++) {
			final Hyperrectangle queryRectangle = RandomQueryRangeGenerator.getRandomQueryRange(range.get(), percentage);
			ranges.add(queryRectangle);
		}
		
		final MultiContinuousJoinQueryClient runable = new MultiContinuousJoinQueryClient(contactPoint, clusterName, 
				streamTable, persistentTable, ranges, udfName, udfValue);
		
		runable.run();
	}
}
