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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.distribution.DistributionGroupConfigurationCache;
import org.bboxdb.distribution.DistributionRegion;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.distribution.membership.BBoxDBInstanceManager;
import org.bboxdb.distribution.partitioner.DistributionGroupZookeeperAdapter;
import org.bboxdb.distribution.partitioner.SpacePartitioner;
import org.bboxdb.distribution.zookeeper.ZookeeperClient;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.distribution.zookeeper.ZookeeperNotFoundException;
import org.bboxdb.misc.Const;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.network.client.future.JoinedTupleListFuture;
import org.bboxdb.network.client.future.TupleListFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
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
	protected CommandLine line;
	
	/**
	 * The connection to the bboxDB Server
	 */
	protected BBoxDB bboxDbConnection;
	
	/**
	 * The pending futures
	 */
	protected final FixedSizeFutureStore pendingFutures;
	
	/**
	 * The amount of pending insert futures
	 */
	protected final static int MAX_PENDING_FUTURES = 5000;
	
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
			final Options options = buildOptions();
			final CommandLineParser parser = new DefaultParser();
			final CommandLine line = parser.parse(options, args);
			
			checkParameter(options, line);
			
			cli = new CLI(line);
			cli.run();
		} catch (ParseException e) {
			System.err.println("Unable to parse commandline arguments: " + e);
			System.exit(-1);
		} finally {
			cli.close();
		}
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
					+ "and cluster (-%s=%s)?\n", CLIParameter.ZOOKEEPER_HOST, zookeeperHost, 
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
			
		case CLIAction.QUERY:
			actionExecuteQuery(line);
			break;
			
		case CLIAction.JOIN:
			actionExecuteJoin(line);
			break;
		
		case CLIAction.CONTINUOUS_QUERY:
			actionExecuteContinuousQuery(line);
			break;
			
		case CLIAction.INSERT:
			actionInsertTuple(line);
			break;
			
		case CLIAction.DELETE:
			actionDeleteTuple(line);
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
			bboxDbConnection.disconnect();
			bboxDbConnection = null;
		}
	}
	
	/**
	 * Create a new table
	 * @param line
	 */
	protected void actionCreateTable(final CommandLine line) {
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
	
			resultFuture.waitForAll();
	
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
	protected void actionDeleteTable(final CommandLine line) {
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Delete table should be performed, but no table was specified");
			printHelpAndExit();
		}
		
		try {
			final String table = line.getOptionValue(CLIParameter.TABLE);
	
			final EmptyResultFuture resultFuture = bboxDbConnection.deleteTable(table);
	
			resultFuture.waitForAll();
	
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
	protected void actionExecuteQuery(final CommandLine line) {
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}
				
		try {
			final TupleListFuture resultFuture = buildQueryFuture(line);
			
			if(resultFuture == null) {
				System.err.println("Unable to get query");
				System.exit(-1);
			}
			
			resultFuture.waitForAll();
			
			if(resultFuture.isFailed()) {
				System.err.println("Unable to execute query: " + resultFuture.getAllMessages());
				System.exit(-1);
			}
			
			for(final Tuple tuple : resultFuture) {
				printTuple(tuple);
			}
			
			System.out.println("Query done");
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
	protected void actionExecuteJoin(final CommandLine line) {
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

			System.out.println("Executing join query...");
			final BoundingBox boundingBox = getBoundingBoxFromArgs(line);	
			final JoinedTupleListFuture resultFuture = bboxDbConnection.queryJoin(tableList, boundingBox);
						
			if(resultFuture == null) {
				System.err.println("Unable to get query");
				System.exit(-1);
			}
			
			resultFuture.waitForAll();
			
			if(resultFuture.isFailed()) {
				System.err.println("Unable to execute query: " + resultFuture.getAllMessages());
				System.exit(-1);
			}
			
			for(final JoinedTuple tuple : resultFuture) {
				printJoinedTuple(tuple);
			}
			
			System.out.println("Join done");
		} catch (BBoxDBException e) {
			System.err.println("Got an exception while performing query: " + e);
			System.exit(-1);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			return;
		}
	}

	/**
	 * Execute a continuous bounding box query
	 * @param line
	 */
	protected void actionExecuteContinuousQuery(final CommandLine line) {
		if(! line.hasOption(CLIParameter.TABLE)) {
			System.err.println("Query should be performed, but no table was specified");
			printHelpAndExit();
		}
				
		try {			
			System.out.println("Executing continuous bounding box query...");
			final String table = line.getOptionValue(CLIParameter.TABLE);

			if(! line.hasOption(CLIParameter.BOUNDING_BOX)) {
				System.err.println("Bounding box is not given");
				System.exit(-1);
			}
			
			final BoundingBox boundingBox = getBoundingBoxFromArgs(line);
			
			final TupleListFuture resultFuture = bboxDbConnection.queryBoundingBoxContinuous
					(table, boundingBox);
			
			if(resultFuture == null) {
				System.err.println("Unable to get query");
				System.exit(-1);
			}
			
			resultFuture.waitForAll();
			
			if(resultFuture.isFailed()) {
				System.err.println("Unable to execute query: " + resultFuture.getAllMessages());
				System.exit(-1);
			}
			
			for(final Tuple tuple : resultFuture) {
				printTuple(tuple);
			}
			
			System.out.println("Query done");
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
	protected void printTuple(final Tuple tuple) {
		System.out.println(tuple.getFormatedString());
	}
	
	/**
	 * Print the given joined tuple
	 * @param joinedTuple
	 */
	protected void printJoinedTuple(final JoinedTuple joinedTuple) {
		System.out.println(joinedTuple.getFormatedString());
	}
	
	/**
	 * Build the query future
	 * @param line
	 * @return
	 * @throws BBoxDBException
	 */
	protected TupleListFuture buildQueryFuture(final CommandLine line)
			throws BBoxDBException {
		
		final String table = line.getOptionValue(CLIParameter.TABLE);

		if(line.hasOption(CLIParameter.KEY)) {
			System.out.println("Executing key query..");
			final String key = line.getOptionValue(CLIParameter.KEY);
			return bboxDbConnection.queryKey(table, key);
			
		} else if(line.hasOption(CLIParameter.BOUNDING_BOX) && line.hasOption(CLIParameter.TIMESTAMP)) {
			System.out.println("Executing bounding box and time query...");
			final BoundingBox boundingBox = getBoundingBoxFromArgs(line);	
			final long timestamp = getTimestampFromArgs();
			return bboxDbConnection.queryBoundingBoxAndTime(table, boundingBox, timestamp);
			
		} else if(line.hasOption(CLIParameter.BOUNDING_BOX)) {
			System.out.println("Executing bounding box query...");
			final BoundingBox boundingBox = getBoundingBoxFromArgs(line);	
			return bboxDbConnection.queryBoundingBox(table, boundingBox);
			
		} else if(line.hasOption(CLIParameter.TIMESTAMP)) { 
			System.out.println("Executing time query...");
			final long timestamp = getTimestampFromArgs();
			return bboxDbConnection.queryVersionTime(table, timestamp);
			
		} else {
			System.err.println("Unable to execute query with the specified parameter");
			printHelpAndExit();
			
			// Unreachable code
			return null;
		}
	}

	/**
	 * Read the timestamp from CLI parameter
	 * @return
	 */
	protected long getTimestampFromArgs() {
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
	protected BoundingBox getBoundingBoxFromArgs(final CommandLine line) {
		final String bbox = line.getOptionValue(CLIParameter.BOUNDING_BOX);
		
		final String[] bboxStringParts = bbox.split(":|,");
		
		if(bboxStringParts.length % 2 != 0) {
			System.err.println("Invalid bounding box: " + bbox);
			System.exit(-1);
		}
		
		final double[] bboxDoubleValues = new double[bboxStringParts.length];
		for(int i = 0; i < bboxStringParts.length; i++) {
			try {
				bboxDoubleValues[i] = Double.parseDouble(bboxStringParts[i]);
			} catch (NumberFormatException e) {
				System.err.println("Invalid number: " + bboxStringParts[i]);
			}
		}
		
		return new BoundingBox(bboxDoubleValues);
	}
	
	/**
	 * Delete a tuple
	 * @param line
	 */
	protected void actionDeleteTuple(final CommandLine line) {
		
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
	protected void checkRequiredArgs(final List<String> requiredArgs) {
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
	protected void actionInsertTuple(final CommandLine line) {
		final List<String> requiredArgs = Arrays.asList(CLIParameter.TABLE,
				CLIParameter.KEY, CLIParameter.BOUNDING_BOX, CLIParameter.VALUE);
		
		checkRequiredArgs(requiredArgs);
		
		final String table = line.getOptionValue(CLIParameter.TABLE);
		final String key = line.getOptionValue(CLIParameter.KEY);
		final String value = line.getOptionValue(CLIParameter.VALUE);
		final BoundingBox boundingBox = getBoundingBoxFromArgs(line);
		
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
	 * Import data
	 * @param line
	 */
	protected void actionImportData(final CommandLine line) {
		
		final List<String> requiredArgs = Arrays.asList(CLIParameter.FILE, 
				CLIParameter.FORMAT, CLIParameter.TABLE);
		
		checkRequiredArgs(requiredArgs);
		
		final String filename = line.getOptionValue(CLIParameter.FILE);
		final String format = line.getOptionValue(CLIParameter.FORMAT);
		final String table = line.getOptionValue(CLIParameter.TABLE);
	
		System.out.println("Importing file: " + filename);
		
		final TupleFileReader tupleFile = new TupleFileReader(filename, format);
		tupleFile.addTupleListener(t -> {
			
			if(t == null) {
				logger.error("Unable to parse line: " + tupleFile.getLastReadLine());
				return;
			}
			
			if(tupleFile.getProcessedLines() % 1000 == 0) {
				System.out.format("Read %d lines\n", tupleFile.getProcessedLines());
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
			System.out.format("Successfully imported %d lines\n", tupleFile.getProcessedLines());
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
	protected void actionDeleteDgroup(final CommandLine line) {
		
		final List<String> requiredArgs = Arrays.asList(CLIParameter.DISTRIBUTION_GROUP);
		
		checkRequiredArgs(requiredArgs);
		
		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);
		
		System.out.println("Deleting distribution group: " + distributionGroup);

		try {
			final EmptyResultFuture future 
				= bboxDbConnection.deleteDistributionGroup(distributionGroup);
			
			future.waitForAll();
			
			if(future.isFailed()) {
				System.err.println("Got an error during distribution group deletion: " 
						+ future.getAllMessages());
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
	protected void actionShowInstances(final CommandLine line) {
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
	protected void actionShowDgroup(final CommandLine line) {
		
		final List<String> requiredArgs = Arrays.asList(CLIParameter.DISTRIBUTION_GROUP);
		
		checkRequiredArgs(requiredArgs);
		
		final String distributionGroup = line.getOptionValue(CLIParameter.DISTRIBUTION_GROUP);
		
		System.out.println("Show distribution group: " + distributionGroup);
		
		try {
			final ZookeeperClient zookeeperClient = ZookeeperClientFactory.getZookeeperClient();
			final DistributionGroupZookeeperAdapter distributionGroupZookeeperAdapter 
				= new DistributionGroupZookeeperAdapter(zookeeperClient);
			
			final SpacePartitioner spacePartitioner = distributionGroupZookeeperAdapter
					.getSpaceparitioner(distributionGroup);
			
			final DistributionGroupConfiguration config = DistributionGroupConfigurationCache
					.getInstance().getDistributionGroupConfiguration(distributionGroup);
			
			final short replicationFactor = config.getReplicationFactor();
			
			System.out.println("Replication factor is: " + replicationFactor);
			
			printDistributionRegionRecursive(spacePartitioner.getRootNode());

		} catch (ZookeeperException | ZookeeperNotFoundException e) {
			System.err.println("Got an exception during reading distribution group:" + e);
			System.exit(-1);
		}
	}
	
	/**
	 * Print the content of the distribution region recursive
	 * @param distributionRegion
	 */
	protected void printDistributionRegionRecursive(final DistributionRegion distributionRegion) {
		
		if(distributionRegion == null) {
			return;
		}
		
		final BoundingBox boundingBox = distributionRegion.getConveringBox();
		
		final String bboxString = IntStream.range(0, boundingBox.getDimension())
			.mapToObj(i -> "Dimension:" + i + " " + boundingBox.getIntervalForDimension(i).toString())
			.collect(Collectors.joining(", "));			
		
		final String systemsString = distributionRegion.getSystems()
				.stream().map(s -> s.getIp() + ":" + s.getPort())
			.collect(Collectors.joining(", ", "[", "]"));	
		
		System.out.format("Region %d, Bounding Box=%s, State=%s, Systems=%s\n",
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
	protected void actionCreateDgroup(final CommandLine line) {
		
		final List<String> requiredArgs = Arrays.asList(CLIParameter.DISTRIBUTION_GROUP, 
				CLIParameter.DIMENSIONS, CLIParameter.REPLICATION_FACTOR);
			
		checkRequiredArgs(requiredArgs);
		
		final String maxRegionSizeString = CLIHelper.getParameterOrDefault(
				line, CLIParameter.MAX_REGION_SIZE, Integer.toString(Const.DEFAULT_MAX_REGION_SIZE));
		
		final String minRegionSizeString = CLIHelper.getParameterOrDefault(
				line, CLIParameter.MIN_REGION_SIZE, Integer.toString(Const.DEFAULT_MIN_REGION_SIZE));
		
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
					.withMaximumRegionSize(maxRegionSize)
					.withMinimumRegionSize(minRegionSize)
					.withPlacementStrategy(resourcePlacement, resourcePlacementConfig)
					.withSpacePartitioner(spacePartitioner, spacePartitionerConfig)
					.build();
					
			final EmptyResultFuture future = bboxDbConnection.createDistributionGroup(
					distributionGroup, configuration);
			
			future.waitForAll();
			
			if(future.isFailed()) {
				System.err.println("Got an error during distribution group creation: " 
						+ future.getAllMessages());
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
	protected static void checkParameter(final Options options, final CommandLine line) {
		
		if(line.hasOption(CLIParameter.HELP)) {
			printHelpAndExit();
		}
		
		if(! line.hasOption(CLIParameter.ACTION)) {
			printHelpAndExit();
		}
		
	}
	
	/**
	 * Build the command line options
	 * @return
	 */
	protected static Options buildOptions() {
		final Options options = new Options();
		
		// Help
		final Option help = Option.builder(CLIParameter.HELP)
				.desc("Show this help")
				.build();
		options.addOption(help);
		
		// Be verbose
		final Option verbose = Option.builder(CLIParameter.VERBOSE)
				.desc("Be verbose")
				.build();
		options.addOption(verbose);
		
		// Action
		final Option action = Option.builder(CLIParameter.ACTION)
				.hasArg()
				.argName("action")
				.desc("The CLI action to execute")
				.build();
		options.addOption(action);
		
		// Host
		final Option host = Option.builder(CLIParameter.ZOOKEEPER_HOST)
				.hasArg()
				.argName("host")
				.desc("The Zookeeper endpoint to connect to (default: 127.0.0.1:2181)")
				.build();
		options.addOption(host);
		
		// Cluster name
		final Option clusterName = Option.builder(CLIParameter.ZOOKEEPER_CLUSTER_NAME)
				.hasArg()
				.argName("clustername")
				.desc("The name of the cluster (default: mycluster)")
				.build();
		options.addOption(clusterName);
		
		// Distribution group
		final Option distributionGroup = Option.builder(CLIParameter.DISTRIBUTION_GROUP)
				.hasArg()
				.argName("distributiongroup")
				.desc("The distribution group")
				.build();
		options.addOption(distributionGroup);
		
		// Dimensions
		final Option dimensions = Option.builder(CLIParameter.DIMENSIONS)
				.hasArg()
				.argName("dimensions")
				.desc("The number of dimensions")
				.build();
		options.addOption(dimensions);
		
		// Replication factor
		final Option replicationFactor = Option.builder(CLIParameter.REPLICATION_FACTOR)
				.hasArg()
				.argName("replicationfactor")
				.desc("The replication factor")
				.build();
		options.addOption(replicationFactor);
		
		// Max region size
		final Option maxRegionSize = Option.builder(CLIParameter.MAX_REGION_SIZE)
				.hasArg()
				.argName("max region size (in MB)")
				.desc("Default: " + Const.DEFAULT_MAX_REGION_SIZE)
				.build();
		options.addOption(maxRegionSize);
		
		// Min region size
		final Option minRegionSize = Option.builder(CLIParameter.MIN_REGION_SIZE)
				.hasArg()
				.argName("min region size (in MB)")
				.desc("Default: " + Const.DEFAULT_MIN_REGION_SIZE)
				.build();
		options.addOption(minRegionSize);
		
		// Resource placement
		final Option resourcePlacement = Option.builder(CLIParameter.RESOURCE_PLACEMENT)
				.hasArg()
				.argName("ressource placement")
				.desc("Default: " + Const.DEFAULT_PLACEMENT_STRATEGY)
				.build();
		options.addOption(resourcePlacement);
		
		// Resource placement config
		final Option resourcePlacementConfig = Option.builder(CLIParameter.RESOURCE_PLACEMENT_CONFIG)
				.hasArg()
				.argName("ressource placement config")
				.desc("Default: " + Const.DEFAULT_PLACEMENT_CONFIG)
				.build();
		options.addOption(resourcePlacementConfig);
		
		// Space partitioner
		final Option spacePartitioner = Option.builder(CLIParameter.SPACE_PARTITIONER)
				.hasArg()
				.argName("space partitioner")
				.desc("Default: " + Const.DEFAULT_SPACE_PARTITIONER)
				.build();
		options.addOption(spacePartitioner);
		
		// Space partitioner
		final Option spacePartitionerConfig = Option.builder(CLIParameter.SPACE_PARTITIONER_CONFIG)
				.hasArg()
				.argName("space partitioner configuration")
				.desc("Default: " + Const.DEFAULT_SPACE_PARTITIONER_CONFIG)
				.build();
		options.addOption(spacePartitionerConfig);
		
		// Table duplicates
		final Option duplplicatesInTable = Option.builder(CLIParameter.DUPLICATES)
				.hasArg()
				.argName("duplicates")
				.desc("Allow duplicates in the table, default: false")
				.build();
		options.addOption(duplplicatesInTable);
		
		// Table ttl
		final Option ttlForTable = Option.builder(CLIParameter.TTL)
				.hasArg()
				.argName("ttl")
				.desc("The TTL of the tuple versions in milliseconds")
				.build();
		options.addOption(ttlForTable);
		
		// Table versions
		final Option versionsForTable = Option.builder(CLIParameter.VERSIONS)
				.hasArg()
				.argName("versions")
				.desc("The amount of versions for a tuple")
				.build();
		options.addOption(versionsForTable);
		
		// Filename
		final Option file = Option.builder(CLIParameter.FILE)
				.hasArg()
				.argName("file")
				.desc("The file to read")
				.build();
		options.addOption(file);
		
		// Format
		final Option format = Option.builder(CLIParameter.FORMAT)
				.hasArg()
				.argName("format")
				.desc("The format of the file")
				.build();
		options.addOption(format);
		
		// Table
		final Option table = Option.builder(CLIParameter.TABLE)
				.hasArg()
				.argName("table")
				.desc("The table to carry out the action")
				.build();
		options.addOption(table);
		
		// Key
		final Option key = Option.builder(CLIParameter.KEY)
				.hasArg()
				.argName("key")
				.desc("The name of the key")
				.build();
		options.addOption(key);
		
		// BBox
		final Option bbox = Option.builder(CLIParameter.BOUNDING_BOX)
				.hasArg()
				.argName("bounding box")
				.desc("The bounding box of the tuple")
				.build();
		options.addOption(bbox);
		
		// Value
		final Option value = Option.builder(CLIParameter.VALUE)
				.hasArg()
				.argName("value")
				.desc("The value of the tuple")
				.build();
		options.addOption(value);
		
		// Time
		final Option time = Option.builder(CLIParameter.TIMESTAMP)
				.hasArg()
				.argName("timestamp")
				.desc("The version time stamp of the tuple")
				.build();
		options.addOption(time);
		
		return options;
	}
	
	/**
	 * Print help and exit the program
	 * @param options 
	 */
	protected static void printHelpAndExit() {
		
		final Options options = buildOptions();
		
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
