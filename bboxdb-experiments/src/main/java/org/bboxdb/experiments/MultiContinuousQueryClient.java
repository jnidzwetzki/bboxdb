/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.QueryPlanBuilder;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.tools.helper.RandomQueryRangeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiContinuousQueryClient implements Runnable {

	/**
	 * The cluster contact point
	 */
	private final String contactPoint;
	
	/**
	 * The name of the cluster
	 */
	private final String clusterName;
	
	/**
	 * The table to query
	 */
	private final String table;
	
	/**
	 * The data ranges
	 */
	private final List<Hyperrectangle> ranges;
	
	/**
	 * All threads
	 */
	private final List<Thread> allThreads = new CopyOnWriteArrayList<>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MultiContinuousQueryClient.class);
	
	public MultiContinuousQueryClient(final String contactPoint, final String clusterName, 
			final String table, final List<Hyperrectangle> ranges) {
			
				this.contactPoint = contactPoint;
				this.clusterName = clusterName;
				this.table = table;
				this.ranges = ranges;
	}
	
	@Override
	public void run() {
		
		try(final BBoxDB connection = new BBoxDBCluster(contactPoint, clusterName)) {
			connection.connect();
			
			for(final Hyperrectangle queryRectangle : ranges) {
				System.out.println("Creating query in range: " + queryRectangle);
				
				final ContinuousQueryPlan queryPlan = QueryPlanBuilder
						.createQueryOnTable(table)
						.compareWithStaticRegion(queryRectangle)
						.forAllNewTuplesStoredInRegion(queryRectangle)
						.build();
				
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
				
				for(final JoinedTuple tuple : queryFuture) {
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
		if(args.length != 5) {
			System.err.println("Usage: <Class> readfile <ClusterContactPoint> <Clustername> <Table> <File>");
			System.exit(-1);
		}
		
		final String contactPoint = args[1];
		final String clusterName = args[2];
		final String table = args[3];
		final File inputFile = new File(args[4]);
		
		 new ArrayList<>();

		try (final Stream<String> lineStream = Files.lines(Paths.get(inputFile.getPath()))) {
	
			final List<Hyperrectangle> ranges = lineStream
					.map(s -> Hyperrectangle
					.fromString(s))
			 		.collect(Collectors.toList());
	
			final MultiContinuousQueryClient runable = new MultiContinuousQueryClient(contactPoint, clusterName, 
					table, ranges);
			
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
		if(args.length != 7) {
			System.err.println("Usage: <Class> autogenerate <ClusterContactPoint> <Clustername> <Table> <Range> <Percentage> <Parallel-Queries>");
			System.exit(-1);
		}
		
		final String contactPoint = args[1];
		final String clusterName = args[2];
		final String table = args[3];
		final String rangeString = args[4];
		final String percentageString = args[5];
		final String parallelQueriesString = args[6];
		
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
		
		final MultiContinuousQueryClient runable = new MultiContinuousQueryClient(contactPoint, clusterName, 
				table, ranges);
		
		runable.run();
	}
}
