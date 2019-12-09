/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.tools.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.partitioner.AbstractTreeSpacePartitoner;
import org.bboxdb.distribution.partitioner.DistributionRegionState;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.partitioner.SpacePartitionerCache;
import org.bboxdb.distribution.region.DistributionRegion;
import org.bboxdb.distribution.zookeeper.DistributionGroupAdapter;
import org.bboxdb.distribution.zookeeper.DistributionRegionAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.misc.Const;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.network.query.ContinuousConstQueryPlan;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.tools.RandomSamplesReader;
import org.bboxdb.tools.TupleFileReader;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The BBoxDB CLI
 *
 */
public class CLI implements Runnable, AutoCloseable {

	/**
	 * The parsed command line
	 */
	private CommandLine line;

	/**
	 * The connection to the bboxDB Server
	 */
	private BBoxDB bboxDbConnection;

	/**
	 * The pending futures
	 */
	private final FixedSizeFutureStore pendingFutures;

	/**
	 * The amount of pending insert futures
	 */
	private final static int MAX_PENDING_FUTURES = 5000;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(CLI.class);

	public CLI(final CommandLine line) {
		this.line = line;

		// The pending future store
		pendingFutures = new FixedSizeFutureStore(MAX_PENDING_FUTURES);

		// Log failed futures
		pendingFutures.addFailedFutureCallback((f) -> {
			logger.error("Failed future detected: {}", f.getAllMessages());
			System.exit(-1);
		});
	}

	/**
	 * Main * Main * Main * Main
	 */
	public static void main(final String[] args) {

		// The cli instance
		CLI cli = null;

		try {
			final Options options = OptionsHelper.buildOptions();
			final CommandLineParser parser = new DefaultParser();
			final CommandLine line = parser.parse(options, args);

			checkParameter(options, line);

			cli = new CLI(line);
			cli.run();
		} catch (ParseException e) {
			System.err.println("Unable to parse commandline arguments: " + e);
			System.exit(-1);
		} finally {
			CloseableHelper.closeWithoutException(cli);
		}
		
		System.exit(0);
	}

	public void run() {
		// Default Zookeeper values
		String zookeeperHost = "localhost:2181";
		String zookeeperClustername = "mycluster";

		if(line.hasOption(CLIParameter.ZOOKEEPER_HOST)) {
			zookeeperHost = line.getOptionValue(CLIParameter.ZOOKEEPER_HOST);
		}

		if(line.hasOption(CLIParameter.ZOOKEEPER_CLUSTER_NAME)) {
			zookeeperClustername = line.getOptionValue(CLIParameter.ZOOKEEPER_CLUSTER_NAME);
		}

		// Connect to zookeeper and BBoxDB
		System.out.print("Connecting to BBoxDB cluster...");
		System.out.flush();
		bboxDbConnection = new BBoxDBCluster(zookeeperHost, zookeeperClustername);

		if( ! bboxDbConnection.connect() ) {
			System.err.println("\n\n");
			System.err.println("Error: Unable to connect to the BBoxDB cluster.");
			System.err.format("Error: Did you specified the correct Zookeeper host (-%s=%s) "
					+ "and cluster (-%s=%s)?%n", CLIParameter.ZOOKEEPER_HOST, zookeeperHost,
					CLIParameter.ZOOKEEPER_CLUSTER_NAME, zookeeperClustername);

			System.exit(-1);
		}

		System.out.println(" [Established]");

		if(line.hasOption(CLIParameter.VERBOSE)) {
			org.apache.log4j.Logger logger4j = org.apache.log4j.Logger.getRootLogger();
			logger4j.setLevel(org.apache.log4j.Level.toLevel("DEBUG"));
		}

		final String action = line.getOptionValue(CLIParameter.ACTION);

		switch (action) {
		case CLIAction.CREATE_DGROUP:
			actionCreateDgroup(line);
			break;

		case CLIAction.DELETE_DGROUP:
			actionDeleteDgroup(line);
			break;

		case CLIAction.SHOW_DGROUP:
			actionShowDgroup(line);
			break;

		case CLIAction.CREATE_TABLE:
			actionCreateTable(line);
			break;

		case CLIAction.DELETE_TABLE:
			actionDeleteTable(line);
			break;

		case CLIAction.SHOW_INSTANCES:
			actionShowInstances(line);
			break;

		case CLIAction.IMPORT:
			actionImportData(line);
			break;

		case CLIAction.QUERY_KEY:
			actionExecuteKeyQuery(line);
			break;
			
		case CLIAction.QUERY_RANGE:
			actionExecuteRangeQuery(line);
			break;
			
		case CLIAction.QUERY_RANGE_TIME:
			actionExecuteRangeAndTimeQuery(line);
			break;
			
		case CLIAction.QUERY_TIME:
			actionExecuteTimeQuery(line);
			break;

		case CLIAction.QUERY_JOIN:
			actionExecuteJoin(line);
			break;

		case CLIAction.QUERY_CONTINUOUS:
			actionExecuteContinuousQuery(line);
			break;

		case CLIAction.INSERT:
			actionInsertTuple(line);
			break;

		case CLIAction.DELETE:
			actionDeleteTuple(line);
			break;
			
		case CLIAction.PREPARTITION:
			prepartition(line);
		break;

		default:
			break;
		}
	}

	/**
	 * Shutdown the instance
	 */
	public void close() {
		if(bboxDbConnection != null) {
			bboxDbConnection.close();
			bboxDbConnection = null;
		}
	}

	/**
	 * Create a new table
	 * @param line
	 */
	private void actionCreateTable(final CommandLine line) {
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Create table should be performed, but no table was specified");
			printHelpAndExit();
		}

		final TupleStoreConfigurationBuilder ssTableConfigurationBuilder = TupleStoreConfigurationBuilder
				.create();

		// Duplicates
		if(line.hasOption(CLIParameter.DUPLICATES)) {
			final String allowDuplicates = line.getOptionValue(CLIParameter.DUPLICATES);

			final boolean duplicatesAllowed = MathUtil.tryParseBooleanOrExit(allowDuplicates,
					() -> "Unable to parse the bolean value for duplicates: " + allowDuplicates);

			ssTableConfigurationBuilder.allowDuplicates(duplicatesAllowed);
		}

		// TTL
		if(line.hasOption(CLIParameter.TTL)) {
			final String ttlString = line.getOptionValue(CLIParameter.TTL);
			final int ttl = MathUtil.tryParseIntOrExit(ttlString,
					() -> "Unable to parse the region size: " + ttlString);
			ssTableConfigurationBuilder.withTTL(ttl, TimeUnit.MILLISECONDS);
		}

		// Versions
		if(line.hasOption(CLIParameter.VERSIONS)) {
			final String versionString = line.getOptionValue(CLIParameter.VERSIONS);
			final int versions = MathUtil.tryParseIntOrExit(versionString,
					() -> "Unable to parse the region size: " + versionString);
			ssTableConfigurationBuilder.withVersions(versions);
		}

		// Spatial index reader
		if(line.hasOption(CLIParameter.SPATIAL_INDEX_READER)) {
			final String spatialIndexReader = line.getOptionValue(CLIParameter.SPATIAL_INDEX_READER);
			ssTableConfigurationBuilder.withSpatialIndexReader(spatialIndexReader);
		}

		// Spatial index writer
		if(line.hasOption(CLIParameter.SPATIAL_INDEX_WRITER)) {
			final String spatialIndexWriter = line.getOptionValue(CLIParameter.SPATIAL_INDEX_WRITER);
			ssTableConfigurationBuilder.withSpatialIndexWriter(spatialIndexWriter);
		}

		final TupleStoreConfiguration configuration = ssTableConfigurationBuilder.build();

		try {
			final String table = line.getOptionValue(CLIParameter.TABLE);

			final EmptyResultFuture resultFuture = bboxDbConnection.createTable(table, configuration);

			resultFuture.waitForCompletion();

			if(resultFuture.isFailed()) {
				System.err.println("Unable to create table: " + resultFuture.getAllMessages());
				System.exit(-1);
			}

		} catch (BBoxDBException e) {
			System.err.println("Got an exception while creating table: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}

	}

	/**
	 * Delete an existing table
	 * @param line
	 */
	private void actionDeleteTable(final CommandLine line) {
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Delete table should be performed, but no table was specified");
			printHelpAndExit();
		}

		try {
			final String table = line.getOptionValue(CLIParameter.TABLE);

			final EmptyResultFuture resultFuture = bboxDbConnection.deleteTable(table);

			resultFuture.waitForCompletion();

			if(resultFuture.isFailed()) {
				System.err.println("Unable to delete table: " + resultFuture.getAllMessages());
				System.exit(-1);
			}

		} catch (BBoxDBException e) {
			System.err.println("Got an exception while deleting table: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

	/**
	 * Execute the given query
	 * @param line
	 */
	private void actionExecuteKeyQuery(final CommandLine line) {
		
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}

		try {
			System.out.println("Executing key query..");
			final String table = line.getOptionValue(CLIParameter.TABLE);
			final String key = line.getOptionValue(CLIParameter.KEY);
			final TupleListFuture resultFuture = bboxDbConnection.queryKey(table, key);
			executeQueryFuture(resultFuture);
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}
	
	/**
	 * Execute the given query
	 * @param line
	 */
	private void actionExecuteRangeQuery(final CommandLine line) {
		
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}

		try {			
			System.out.println("Executing range query...");
			final String table = line.getOptionValue(CLIParameter.TABLE);
			final Hyperrectangle boundingBox = getBoundingBoxFromArgs(line);
			
			// Custom filter parameter
			final String customFilterClass = CLIHelper.getParameterOrDefault(line, 
					CLIParameter.CUSTOM_FILTER_CLASS, "");
			
			final String customFilterValue = CLIHelper.getParameterOrDefault(line, 
					CLIParameter.CUSTOM_FILTER_VALUE, "");
			
			final TupleListFuture resultFuture = bboxDbConnection.queryRectangle(table, boundingBox, 
					customFilterClass, customFilterValue.getBytes());
			
			executeQueryFuture(resultFuture);
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}
	
	/**
	 * Execute the given query
	 * @param line
	 */
	private void actionExecuteRangeAndTimeQuery(final CommandLine line) {
		
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}

		try {
			System.out.println("Executing range and time query...");
			final String table = line.getOptionValue(CLIParameter.TABLE);
			final Hyperrectangle boundingBox = getBoundingBoxFromArgs(line);
			final long timestamp = getTimestampFromArgs();
			final TupleListFuture resultFuture = bboxDbConnection.queryRectangleAndTime(
					table, boundingBox, timestamp);

			executeQueryFuture(resultFuture);
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}
	
	/**
	 * Execute the given query
	 * @param line
	 */
	private void actionExecuteTimeQuery(final CommandLine line) {
		
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}

		try {			
			System.out.println("Executing time query...");
			final String table = line.getOptionValue(CLIParameter.TABLE);
			final long timestamp = getTimestampFromArgs();
			final TupleListFuture resultFuture = bboxDbConnection.queryVersionTime(table, timestamp);
			executeQueryFuture(resultFuture);
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}




	/**
	 * Execute the query future
	 * @param resultFuture
	 * @throws InterruptedException
	 */
	private void executeQueryFuture(final TupleListFuture resultFuture) throws InterruptedException {
		if(resultFuture == null) {
			System.err.println("Unable to get query");
			System.exit(-1);
		}

		resultFuture.waitForCompletion();

		if(resultFuture.isFailed()) {
			System.err.println("Unable to execute query: " + resultFuture.getAllMessages());
			System.exit(-1);
		}

		long resultTuples = 0;
		for(final Tuple tuple : resultFuture) {
			printTuple(tuple);
			resultTuples++;
		}

		System.out.format("Query done - got %d tuples back%n", resultTuples);
	}

	/**
	 * Execute the given query
	 * @param line
	 */
	private void actionExecuteJoin(final CommandLine line) {
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}

		if(! line.hasOption(CLIParameter.BOUNDING_BOX)) {
			System.err.println("Bounding box is not given");
			System.exit(-1);
		}

		try {
			final String tables = line.getOptionValue(CLIParameter.TABLE);
			final List<String> tableList = Arrays.asList(tables.split(":"));
			
			final Hyperrectangle boundingBox = getBoundingBoxFromArgs(line);

			final JoinedTupleListFuture resultFuture1 = executeJoin(tableList, boundingBox);
		
			processJoinedTupleList(boundingBox, resultFuture1);
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

	/**
	 * Print the given tuple list
	 * @param boundingBox
	 * @param resultFuture1
	 * @return 
	 */
	private void processJoinedTupleList(final Hyperrectangle boundingBox, 
			final JoinedTupleListFuture resultFuture1) {
		
		long resultTuples = 0;
				
		for(final JoinedTuple tuple : resultFuture1) {
			
			assert tuple.getBoundingBox().intersects(boundingBox) : "Bounding box mismatch: " + tuple.getBoundingBox();
			
			final Hyperrectangle box1 = tuple.getTuple(0).getBoundingBox();
			final Hyperrectangle box2 = tuple.getTuple(1).getBoundingBox();
			assert box1.intersects(boundingBox) : "BBox 1 tuple mismatch: " + box1.toCompactString();
			assert box2.intersects(boundingBox) : "BBox 2 tuple mismatch: " + box2.toCompactString();
			assert box2.intersects(box1) : "Overlap mismatch: " + box1.toCompactString() 
				+ " " + box2.toCompactString();
			
			printJoinedTuple(tuple);
			resultTuples++;
		}

		System.out.format("Join done - got %d tuples back%n", resultTuples);
	}

	/**
	 * Execute a join
	 * @param tableList
	 * @param boundingBox
	 * @return
	 * @throws BBoxDBException
	 * @throws InterruptedException
	 */
	private JoinedTupleListFuture executeJoin(final List<String> tableList, 
			final Hyperrectangle boundingBox) throws BBoxDBException, InterruptedException {
		
		System.out.println("Executing join query...");
		
		// Custom filter parameter
		final String customFilterClass = CLIHelper.getParameterOrDefault(line, CLIParameter.CUSTOM_FILTER_CLASS, "");
		final String customFilterValue = CLIHelper.getParameterOrDefault(line, CLIParameter.CUSTOM_FILTER_VALUE, "");
								
		final JoinedTupleListFuture resultFuture = bboxDbConnection.queryJoin(tableList, boundingBox,
				customFilterClass, customFilterValue.getBytes());

		if(resultFuture == null) {
			System.err.println("Unable to get query");
			System.exit(-1);
		}

		resultFuture.waitForCompletion();

		if(resultFuture.isFailed()) {
			System.err.println("Unable to execute query: " + resultFuture.getAllMessages());
			System.exit(-1);
		}
		
		return resultFuture;
	}

	/**
	 * Execute a continuous bounding box query
	 * @param line
	 */
	private void actionExecuteContinuousQuery(final CommandLine line) {
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}

		try {
			System.out.println("Executing continuous range query...");
			final String table = line.getOptionValue(CLIParameter.TABLE);

			if(! line.hasOption(CLIParameter.BOUNDING_BOX)) {
				System.err.println("Bounding box is not given");
				System.exit(-1);
			}

			final Hyperrectangle boundingBox = getBoundingBoxFromArgs(line);
			
			final ContinuousConstQueryPlan constQueryPlan = new ContinuousConstQueryPlan(table, 
					new ArrayList<>(), boundingBox, boundingBox, true);
		
			final JoinedTupleListFuture resultFuture = bboxDbConnection.queryContinuous(constQueryPlan);

			if(resultFuture == null) {
				System.err.println("Unable to get query");
				System.exit(-1);
			}

			resultFuture.waitForCompletion();

			if(resultFuture.isFailed()) {
				System.err.println("Unable to execute query: " + resultFuture.getAllMessages());
				System.exit(-1);
			}

			long resultTuples = 0;
			for(final JoinedTuple tuple : resultFuture) {
				printJoinedTuple(tuple);
				resultTuples++;
			}

			System.out.format("Query done - got %d tuples back%n", resultTuples);
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

	/**
	 * Print the given tuple
	 * @param tuple
	 */
	private void printTuple(final Tuple tuple) {
		System.out.println(tuple.getFormatedString());
	}

	/**
	 * Print the given joined tuple
	 * @param joinedTuple
	 */
	private void printJoinedTuple(final JoinedTuple joinedTuple) {
		System.out.println(joinedTuple.getFormatedString());
	}

	/**
	 * Read the timestamp from CLI parameter
	 * @return
	 */
	private long getTimestampFromArgs() {
		final String timestampString = line.getOptionValue(CLIParameter.TIMESTAMP);
		long value = -1;

		try {
			value = Long.parseLong(timestampString);
		} catch (NumberFormatException e) {
			System.err.println("Unable to parse timestamp: "+ timestampString);
			printHelpAndExit();
		}

		return value;
	}

	/**
	 * Read and convert the bounding box from CLI args
	 * @param line
	 * @return
	 */
	private Hyperrectangle getBoundingBoxFromArgs(final CommandLine line) {
		final String bbox = line.getOptionValue(CLIParameter.BOUNDING_BOX);
		final Optional<Hyperrectangle> resultBox = HyperrectangleHelper.parseBBox(bbox);
		
		if(! resultBox.isPresent()) {
			System.err.println("Invalid bounding box: " + bbox);
			System.exit(-1);
		}
		
		return resultBox.get();
	}

	/**
	 * Delete a tuple
	 * @param line
	 */
	private void actionDeleteTuple(final CommandLine line) {

		if(! line.hasOption(CLIParameter.KEY) || ! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Key or table are missing");
			printHelpAndExit();
		}

		final String key = line.getOptionValue(CLIParameter.KEY);
		final String table = line.getOptionValue(CLIParameter.TABLE);

		System.out.println("Deleting tuple for key: " + key);

		try {
			final EmptyResultFuture resultFuture = bboxDbConnection.deleteTuple(table, key);
			pendingFutures.put(resultFuture);
			pendingFutures.waitForCompletion();
		} catch (BBoxDBException e) {
			System.err.println("Got an error during delete: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

	/**
	 * Check the required args
	 * @param requiredArgs
	 */
	private void checkRequiredArgs(final List<String> requiredArgs) {
		for(final String arg : requiredArgs) {
			if(! line.hasOption(arg)) {
				System.err.println("Option is missing: " + arg);
				printHelpAndExit();
			}
		}
	}

	/**
	 * Insert a new tuple
	 * @param line
	 */
	private void actionInsertTuple(final CommandLine line) {
		final List<String> requiredArgs = Arrays.asList(CLIParameter.TABLE,
				CLIParameter.KEY, CLIParameter.BOUNDING_BOX, CLIParameter.VALUE);

		checkRequiredArgs(requiredArgs);

		final String table = line.getOptionValue(CLIParameter.TABLE);
		final String key = line.getOptionValue(CLIParameter.KEY);
		final String value = line.getOptionValue(CLIParameter.VALUE);
		final Hyperrectangle boundingBox = getBoundingBoxFromArgs(line);

		final Tuple tuple = new Tuple(key, boundingBox, value.getBytes());

		System.out.println("Insert new tuple into table: " + table);

		try {
			final EmptyResultFuture future = bboxDbConnection.insertTuple(table, tuple);
			pendingFutures.put(future);
			pendingFutures.waitForCompletion();
		} catch (BBoxDBException e) {
			System.err.println("Got an error during insert: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}
	
	/**
	 * Prepartition the given distribution group
	 * @param line
	 */
	private void prepartition(final CommandLine line) {
		final List<String> requiredArgs = Arrays.asList(CLIParameter.FILE,
				CLIParameter.FORMAT, CLIParameter.DISTRIBUTION_GROUP, CLIParameter.PARTITIONS);

		checkRequiredArgs(requiredArgs);

		final String filename = line.getOptionValue(CLIParameter.FILE);
		final String format = line.getOptionValue(CLIParameter.FORMAT);		
		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);
		final String parititonsString = line.getOptionValue(CLIParameter.PARTITIONS);

		final int partitions = MathUtil.tryParseIntOrExit(parititonsString,
				() -> "Unable to parse the partitions: " + parititonsString);
	
		try {
			exitIfGroupDoesNotExist(distributionGroup);		
			checkForExistingPartitioning(distributionGroup);
			
			final List<Hyperrectangle> allSamples = RandomSamplesReader.readSamplesRandom(
					filename, format, 0.1);
			
			System.out.println("We have read the following amount of bounding boxes: " + allSamples.size());
		
			final SpacePartitioner partitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(distributionGroup);
			
			if(! (partitioner instanceof AbstractTreeSpacePartitoner)) {
				System.err.println("Unsupported space partitoner: " + partitioner);
				System.exit(-1);
			}
			
			final AbstractTreeSpacePartitoner spacePartitioner = (AbstractTreeSpacePartitoner) partitioner;

			final DistributionRegionAdapter adapter 
				= ZookeeperClientFactory.getZookeeperClient().getDistributionRegionAdapter();
			
			// The active regions and the samples
			final Map<DistributionRegion, List<Hyperrectangle>> activeRegions = new HashMap<>();
			final List<DistributionRegion> readActiveRegions = getActiveRegions(spacePartitioner);
			
			if(readActiveRegions.size() != 1) {
				System.err.println("Read more then one active region: " + readActiveRegions.size());
				System.exit(-1);
			}
			
			activeRegions.put(readActiveRegions.get(0), allSamples);
			
			while(activeRegions.keySet().size() < partitions) {
				
				//System.out.format("We have now %s of %s active partitions, executing split %n", 
				//		activeRegions.size(), partitions);		
				
				// Get the region with the highest amount of contained samples
				final DistributionRegion regionToSplit = activeRegions.entrySet()
					.stream()
					.max((entry1, entry2) -> entry1.getValue().size() > entry2.getValue().size() ? 1 : -1)
					.get().getKey();
				
				System.out.format("Splitting region %d%n", regionToSplit.getRegionId());
				
				final List<DistributionRegion> newRegions 
					= spacePartitioner.splitRegion(regionToSplit, activeRegions.get(regionToSplit));
				
				spacePartitioner.waitForSplitCompleteZookeeperCallback(regionToSplit, 2);
				spacePartitioner.splitComplete(regionToSplit, newRegions);
				spacePartitioner.waitUntilNodeStateIs(regionToSplit, DistributionRegionState.SPLIT);
				
				// Prevent merging of nodes
				for(final DistributionRegion region : newRegions) {
					adapter.setMergingSupported(region, false);
				}
				
				// Redistribute samples
				newRegions.forEach(d -> activeRegions.put(d, new ArrayList<>()));
				final List<Hyperrectangle> oldSamples = activeRegions.remove(regionToSplit);
				
				if(oldSamples.isEmpty()) {
					System.err.println("Got empty samples for: " + regionToSplit.getIdentifier());
				}
				
				for(final Hyperrectangle sample : oldSamples) {
					for(final DistributionRegion region : newRegions) {
						if(region.getConveringBox().intersects(sample)) {
							activeRegions.get(region).add(sample);
						}
					}
				}				
			}
			
			printSampleDistribution(activeRegions);

		} catch (Exception e) {
			logger.error("Got an exception", e);
			System.exit(-1);
		}	
	}
	
	/**
	 * Print the
	 * @param activeRegions
	 */
	private void printSampleDistribution(
			final Map<DistributionRegion, List<Hyperrectangle>> activeRegions) {
		
		System.out.println("=============================================");
		System.out.println("Distribution of samples after partitioning");
		System.out.println("=============================================");
		
		final long maxValue = activeRegions.values()
				.stream()
				.mapToLong(l -> l.size())
				.max()
				.getAsLong();
		
		final double dots = 30.0;
		
		final int maxRegionString = activeRegions.keySet()
				.stream()
				.map(s -> s.getIdentifier())
				.mapToInt(i -> i.length())
				.max()
				.orElse(0);
		
		for(final Entry<DistributionRegion, List<Hyperrectangle>> entry : activeRegions.entrySet()) {
			
			final DistributionRegion region = entry.getKey();
			final List<Hyperrectangle> samples = entry.getValue();

			final String identifier = region.getIdentifier();
			System.out.print("Region: '" + identifier + "' ");
			
			for(int i = 0; i < maxRegionString - identifier.length(); i++) {
				System.out.print(" ");
			}
			
			final double dotsForRegionDouble = (((double) samples.size() / (double) maxValue) * dots);
			final int dotsForRegion = (int) MathUtil.round(dotsForRegionDouble, 0);
			
			for(int i = 0; i < dotsForRegion; i++) {
				System.out.print("*");
			}
			
			for(int i = 0; i < (dots - dotsForRegion); i++) {
				System.out.print(" ");
			}
			
			System.out.println(" (samples: "+ samples.size() + ")");
		}
		
		System.out.println("\n");
	}

	/**
	 * Does the group exist?
	 * @param distributionGroup
	 * @throws ZookeeperException
	 * @throws ZookeeperNotFoundException
	 */
	private static void exitIfGroupDoesNotExist(final String distributionGroup)
			throws ZookeeperException, ZookeeperNotFoundException {
		
		final DistributionGroupAdapter adapter 
			= ZookeeperClientFactory.getZookeeperClient().getDistributionGroupAdapter();
		
		final List<String> knownGroups = adapter.getDistributionGroups();
		
		if(! knownGroups.contains(distributionGroup)) {
			System.err.format("Distribution group %s does not exist%n", distributionGroup);
			System.exit(-1);
		}
	}

	/**
	 * Is the region already partitioned?
	 * @param distributionGroup
	 * @throws BBoxDBException
	 */
	private static void checkForExistingPartitioning(final String distributionGroup)
			throws BBoxDBException {
		
		final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
				.getSpacePartitionerForGroupName(distributionGroup);
		
		if(spacePartitioner.getRootNode().getThisAndChildRegions().size() != 1) {
			System.err.println("Region is already splitted unable to use this for inital splitting");
			System.exit(-1);
		}
	}

	/**
	 * Get the active regions
	 * @param spacePartitioner
	 * @return
	 * @throws BBoxDBException
	 */
	private List<DistributionRegion> getActiveRegions(final SpacePartitioner spacePartitioner) 
			throws BBoxDBException {
		
		return spacePartitioner.getRootNode()
				.getThisAndChildRegions()
				.stream()
				.filter(r -> r.getState() == DistributionRegionState.ACTIVE)
				.collect(Collectors.toList());
	}
	
	/**
	 * Import data
	 * @param line
	 */
	private void actionImportData(final CommandLine line) {

		final List<String> requiredArgs = Arrays.asList(CLIParameter.FILE,
				CLIParameter.FORMAT, CLIParameter.TABLE);

		checkRequiredArgs(requiredArgs);

		final String filename = line.getOptionValue(CLIParameter.FILE);
		final String format = line.getOptionValue(CLIParameter.FORMAT);
		final String table = line.getOptionValue(CLIParameter.TABLE);

		final String paddingString
			= CLIHelper.getParameterOrDefault(line, CLIParameter.BOUNDING_BOX_PADDING, "0.0");

		final double padding = MathUtil.tryParseDoubleOrExit(paddingString,
				() -> "Untable to parse: " + paddingString);

		System.out.format("Importing file: %s with padding %f%n", filename, padding);

		final TupleFileReader tupleFile = new TupleFileReader(filename, format, padding);
		tupleFile.addTupleListener(t -> {

			if(tupleFile.getProcessedLines() % 1000 == 0) {
				System.out.format("Read %d lines%n", tupleFile.getProcessedLines());
			}

			try {
				final EmptyResultFuture result = bboxDbConnection.insertTuple(table, t);
				pendingFutures.put(result);

			} catch (BBoxDBException e) {
				logger.error("Got exception while inserting tuple", e);
			}
		});

		try {
			tupleFile.processFile();
			pendingFutures.waitForCompletion();
			final long skippedLines = tupleFile.getSkippedLines();
			final long processedLines = tupleFile.getProcessedLines();

			System.out.format("Successfully imported %d lines (and skipped %d invalid lines) %n",
					processedLines - skippedLines, skippedLines);
		} catch (IOException e) {
			logger.error("Got IO Exception while reading data", e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

	/**
	 * Delete a distribution group
	 * @param line
	 */
	private void actionDeleteDgroup(final CommandLine line) {

		final List<String> requiredArgs = Arrays.asList(CLIParameter.DISTRIBUTION_GROUP);

		checkRequiredArgs(requiredArgs);

		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);

		System.out.println("Deleting distribution group: " + distributionGroup);

		try {
			final EmptyResultFuture future
				= bboxDbConnection.deleteDistributionGroup(distributionGroup);

			future.waitForCompletion();

			if(future.isFailed()) {
				System.err.println("Got an error during distribution group deletion: "
						+ future.getAllMessages());
				System.exit(-1);
			}
		}  catch (BBoxDBException e) {
			System.err.println("Got an exception during distribution group creation: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			System.err.println("Waiting was interrupted");
			System.exit(-1);
		}
	}

	/**
	 * Show all discovered instances
	 * @param line
	 */
	private void actionShowInstances(final CommandLine line) {
		System.out.println("Show all discovered BBoxDB instances");

		final BBoxDBInstanceManager distributedInstanceManager = BBoxDBInstanceManager.getInstance();
		final List<BBoxDBInstance> allInstances = distributedInstanceManager.getInstances();

		allInstances.sort((i1, i2) -> i1.getIp().compareTo(i2.getIp()));

		System.out.println();
		System.out.println("#######");
		allInstances.forEach(i -> System.out.println(i));
		System.out.println("#######");
	}


	/**
	 * Show a distribution group
	 * @param line
	 */
	private void actionShowDgroup(final CommandLine line) {

		final List<String> requiredArgs = Arrays.asList(CLIParameter.DISTRIBUTION_GROUP);

		checkRequiredArgs(requiredArgs);

		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);

		System.out.println("Show distribution group: " + distributionGroup);

		try {
			final SpacePartitioner spacePartitioner = SpacePartitionerCache.getInstance()
					.getSpacePartitionerForGroupName(distributionGroup);

			final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
					.getInstance().getDistributionGroupConfiguration(distributionGroup);

			final short replicationFactor = config.getReplicationFactor();

			System.out.println("Replication factor is: " + replicationFactor);

			printDistributionRegionRecursive(spacePartitioner.getRootNode());

		} catch (BBoxDBException | ZookeeperNotFoundException e) {
			System.err.println("Got an exception during reading distribution group:" + e);
			System.exit(-1);
		}
	}

	/**
	 * Print the content of the distribution region recursive
	 * @param distributionRegion
	 */
	private void printDistributionRegionRecursive(final DistributionRegion distributionRegion) {

		if(distributionRegion == null) {
			return;
		}

		final Hyperrectangle boundingBox = distributionRegion.getConveringBox();

		final String bboxString = IntStream.range(0, boundingBox.getDimension())
			.mapToObj(i -> "Dimension:" + i + " " + boundingBox.getIntervalForDimension(i).toString())
			.collect(Collectors.joining(", "));

		final String systemsString = distributionRegion.getSystems()
				.stream().map(s -> s.getIp() + ":" + s.getPort())
			.collect(Collectors.joining(", ", "[", "]"));

		System.out.format("Region %d, Bounding Box=%s, State=%s, Systems=%s%n",
				distributionRegion.getRegionId(), bboxString,
				distributionRegion.getState(), systemsString);

		for(final DistributionRegion region : distributionRegion.getDirectChildren()) {
			printDistributionRegionRecursive(region);
		}
	}

	/**
	 * Create a new distribution group
	 * @param line
	 */
	private void actionCreateDgroup(final CommandLine line) {

		final List<String> requiredArgs = Arrays.asList(CLIParameter.DISTRIBUTION_GROUP,
				CLIParameter.DIMENSIONS, CLIParameter.REPLICATION_FACTOR);

		checkRequiredArgs(requiredArgs);

		final String maxRegionSizeString = CLIHelper.getParameterOrDefault(
				line, CLIParameter.MAX_REGION_SIZE, Integer.toString(Const.DEFAULT_MAX_REGION_SIZE_IN_MB));

		final String minRegionSizeString = CLIHelper.getParameterOrDefault(
				line, CLIParameter.MIN_REGION_SIZE, Integer.toString(Const.DEFAULT_MIN_REGION_SIZE_IN_MB));

		final int maxRegionSize = MathUtil.tryParseIntOrExit(maxRegionSizeString,
				() -> "Unable to parse the max region size: " + maxRegionSizeString);

		final int minRegionSize = MathUtil.tryParseIntOrExit(minRegionSizeString,
				() -> "Unable to parse the min region size: " + minRegionSizeString);

		final String resourcePlacement = CLIHelper.getParameterOrDefault(
				line, CLIParameter.RESOURCE_PLACEMENT, Const.DEFAULT_PLACEMENT_STRATEGY);

		final String resourcePlacementConfig = CLIHelper.getParameterOrDefault(
				line, CLIParameter.RESOURCE_PLACEMENT_CONFIG, Const.DEFAULT_PLACEMENT_CONFIG);

		final String spacePartitioner = CLIHelper.getParameterOrDefault(
				line, CLIParameter.SPACE_PARTITIONER, Const.DEFAULT_SPACE_PARTITIONER);

		final String spacePartitionerConfig = CLIHelper.getParameterOrDefault(
				line, CLIParameter.SPACE_PARTITIONER_CONFIG, Const.DEFAULT_SPACE_PARTITIONER_CONFIG);

		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);

		final String replicationFactorString = line.getOptionValue(CLIParameter.REPLICATION_FACTOR);

		final int replicationFactor = MathUtil.tryParseIntOrExit(replicationFactorString,
				() -> "This is not a valid replication factor: " + replicationFactorString);

		final String dimensionsString = line.getOptionValue(CLIParameter.DIMENSIONS);

		final int dimensions = MathUtil.tryParseIntOrExit(dimensionsString,
				() -> "This is not a valid dimension: " + dimensionsString);

		System.out.println("Create new distribution group: " + distributionGroup);

		try {
			final DistributionGroupConfiguration configuration = DistributionGroupConfigurationBuilder
					.create(dimensions)
					.withReplicationFactor((short) replicationFactor)
					.withMaximumRegionSizeInMB(maxRegionSize)
					.withMinimumRegionSizeInMB(minRegionSize)
					.withPlacementStrategy(resourcePlacement, resourcePlacementConfig)
					.withSpacePartitioner(spacePartitioner, spacePartitionerConfig)
					.build();

			final EmptyResultFuture future = bboxDbConnection.createDistributionGroup(
					distributionGroup, configuration);

			future.waitForCompletion();

			if(future.isFailed()) {
				System.err.println("Got an error during distribution group creation: "
						+ future.getAllMessages());
				System.exit(-1);
			}

		} catch (BBoxDBException e) {
			System.err.println("Got an exception during distribution group creation: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			System.err.println("Waiting was interrupted");
			System.exit(-1);
		}
	}

	/**
	 * Check the command line for all needed parameter
	 * @param options
	 * @param line
	 */
	private static void checkParameter(final Options options, final CommandLine line) {

		if(line.hasOption(CLIParameter.HELP)) {
			printHelpAndExit();
		}

		if(! line.hasOption(CLIParameter.ACTION)) {
			printHelpAndExit();
		}
	}

	/**
	 * Print help and exit the program
	 * @param options
	 */
	private static void printHelpAndExit() {

		final Options options = OptionsHelper.buildOptions();

		final String allActions = CLIAction.ALL_ACTIONS
				.stream().collect(Collectors.joining(", ", "[", "]"));

		final String allBuilder = TupleBuilderFactory.ALL_BUILDER
				.stream().collect(Collectors.joining(", ", "[", "]"));

		final String header = "BBoxDB command line interace (CLI)\n\n"
				+ "Available actions are: " + allActions + "\n"
				+ "Supported import formats: " + allBuilder + "\n\n";

		final String footer = "\nPlease report issues at https://github.com/jnidzwetzki/bboxdb/issues\n";

		final HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(120);
		formatter.printHelp("CLI", header, options, footer);

		System.exit(-1);
	}

}