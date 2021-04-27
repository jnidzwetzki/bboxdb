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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.distribution.zookeeper.TupleStoreAdapter;
import org.bboxdb.distribution.zookeeper.ZookeeperClientFactory;
import org.bboxdb.distribution.zookeeper.ZookeeperException;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.tools.helper.RandomQueryRangeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractMultiQueryClient {

	/**
	 * The name of the cluster
	 */
	protected final String clusterName;
	
	/**
	 * The data ranges
	 */
	protected final List<Hyperrectangle> ranges;
	
	/**
	 * All threads
	 */
	protected final List<Thread> allThreads = new CopyOnWriteArrayList<>();

	/**
	 * The UDF name
	 */
	protected final Optional<String> udfName;
	
	/**
	 * The UDF value
	 */
	protected final Optional<String> udfValue;
	
	/**
	 * The cluster contact point
	 */
	protected final String contactPoint;
	
	/**
	 * The table to query
	 */
	protected final String streamTable;
	
	/**
	 * The number of received results
	 */
	protected final AtomicLong receivedResults = new AtomicLong(0);
	
	/**
	 * The Logger
	 */
	final static Logger logger = LoggerFactory.getLogger(AbstractMultiQueryClient.class);

	public AbstractMultiQueryClient(final String clusterName, final String contactPoint, 
			final String streamTable, final List<Hyperrectangle> ranges, 
			final Optional<String> udfName, final Optional<String> udfValue) {
		
		super();
		this.clusterName = clusterName;
		this.ranges = ranges;
		this.udfName = udfName;
		this.udfValue = udfValue;
		this.contactPoint = contactPoint;
		this.streamTable = streamTable;
	}
	
	/**
	 * Is the table known
	 * @param tablename
	 * @throws ZookeeperException
	 * @throws StorageManagerException
	 */
	protected void isTableKnown(final String tablename) throws ZookeeperException, StorageManagerException {
		final TupleStoreAdapter tupleStoreAdapter = ZookeeperClientFactory
				.getZookeeperClient().getTupleStoreAdapter();
		
		final TupleStoreName tupleStoreName = new TupleStoreName(tablename);
		
		if(! tupleStoreAdapter.isTableKnown(tupleStoreName)) {
			throw new StorageManagerException("Table: " + tupleStoreName.getFullname() + " is unknown");
		}
	}

	/**
	 * Write the queries into a file for later usage
	 * @param args
	 */
	protected static void performWriteData(final String[] args) {
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
	 * Dump the result counter
	 * @throws InterruptedException 
	 */
	protected void dumpResultCounter() throws InterruptedException {
		while(true) {
			logger.info("Got {} number of results back in total", receivedResults.get());
			Thread.sleep(1000);
		}
	}

}
