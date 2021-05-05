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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.QueryPlanBuilder;
import org.bboxdb.network.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.tools.helper.RandomQueryRangeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiContinuousJoinQueryClient extends AbstractMultiQueryClient implements Runnable {

	/**
	 * The persistent table to query
	 */
	private final String persistentTable;
		
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MultiContinuousJoinQueryClient.class);


	public MultiContinuousJoinQueryClient(final String contactPoint, final String clusterName, 
			final String streamTable, final String persistentTable, final List<Hyperrectangle> ranges, 
			final Optional<String> udfName, 
			final Optional<String> udfValue) {
		
		super(clusterName, contactPoint, streamTable, ranges, udfName, udfValue);
		this.persistentTable = persistentTable;
	}
	
	@Override
	public void run() {
		
		try(final BBoxDB connection = new BBoxDBCluster(contactPoint, clusterName)) {
			connection.connect();

			isTableKnown(persistentTable);
			isTableKnown(streamTable);
			
			for(final Hyperrectangle queryRectangle : ranges) {
				System.out.println("Creating query in range: " + queryRectangle);
				
				final QueryPlanBuilder queryPlanBuilder = QueryPlanBuilder
						.createQueryOnTable(streamTable)
						.spatialJoinWithTable(persistentTable)
	//					.enlargeStreamTupleBoundBoxByFactor(2)
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
			
			dumpResultCounter();
			
			for(final Thread thread : allThreads) {
				thread.join();
			}
		} catch (BBoxDBException e) {
			logger.error("Got an exception", e);
		} catch (InterruptedException e) {
			return;
		} catch (ZookeeperException e) {
			logger.error("Got an exception", e);
		} catch (StorageManagerException e) {
			logger.error("Got an exception", e);
		} 
	}
	
	/**
	 * 
	 * @param queryFuture
	 */
	private void readResultsInThread(final JoinedTupleListFuture queryFuture) {
		final Thread thread = new Thread(() -> {
			
			try {
				logger.info("Wait for query ready");
				queryFuture.waitForCompletion();
				logger.info("Wait for query ready - DONE");
				
				if(queryFuture.isFailed()) {
					logger.error("Query error {}", queryFuture.getAllMessages());
					return;
				}
				
				for(final MultiTuple tuple : queryFuture) {
					logger.debug("Got tuple {} back", tuple);
					receivedResults.incrementAndGet();
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
