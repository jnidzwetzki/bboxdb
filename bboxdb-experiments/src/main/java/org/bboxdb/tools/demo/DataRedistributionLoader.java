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
package org.bboxdb.tools.demo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.tools.converter.tuple.GeoJSONTupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataRedistributionLoader implements Runnable {

	/**
	 * The files to load
	 */
	private final String[] files;

	/**
	 * The BBoxDB cluster connection
	 */
	private BBoxDBCluster bboxDBCluster;

	/**
	 * The pending futures
	 */
	private final FixedSizeFutureStore pendingFutures;

	/**
	 * The currently loaded files
	 */
	private final List<String> loadedFiles;
	
	/**
	 * The already processed files
	 */
	private final Set<String> processedFiles;

	/**
	 * The GEOJSON tuple builder
	 */
	private final TupleBuilder tupleBuilder;

	/**
	 * The amount of pending insert futures
	 */
	private final static int MAX_PENDING_FUTURES = 1000;

	/**
	 * The name of the distribution group
	 */
	private final static String DGROUP = "demogroup";

	/**
	 * The name of the data table
	 */
	private final static String TABLE = DGROUP + "_osmtable";

	/**
	 * The number of files to load
	 */
	private final int numberOfFilesToLoad;

	/**
	 * The number of max parallel loaded files
	 */
	private final int numberOfMaxLoadedFiles;
	
	/**
	 * The underflow size in MB
	 */
	private final int underflowSize;

	/**
	 * The overflow size in MB
	 */
	private final int overflowSize;
	
	/**
	 * The nowait flag
	 */
	private final boolean nowait;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DataRedistributionLoader.class);



	public DataRedistributionLoader(final String files, final int numberOfFilesToLoad,
			final int numberOfMaxLoadedFiles, final int underflowSize, 
			final int overflowSize, final BBoxDBCluster bboxDBCluster, final boolean nowait) {

		this.numberOfFilesToLoad = numberOfFilesToLoad;
		this.numberOfMaxLoadedFiles = numberOfMaxLoadedFiles;
		this.bboxDBCluster = bboxDBCluster;
		this.underflowSize = underflowSize;
		this.overflowSize = overflowSize;
		this.nowait = nowait;
		
		this.loadedFiles = new CopyOnWriteArrayList<>();
		this.processedFiles = new HashSet<>();
		this.pendingFutures = new FixedSizeFutureStore(MAX_PENDING_FUTURES, true);
		this.files = files.split(":");
		this.tupleBuilder = new GeoJSONTupleBuilder();
	}

	/**
	 * Execute the loader
	 */
	@Override
	public void run() {
		checkFilesExist();
		initBBoxDB();

		try {
			final File tempFile = File.createTempFile("performance-", ".tmp");
			logger.info("Performance data is logged to {}", tempFile);

			final BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile));
			pendingFutures.writeStatistics(bw);

			for(int i = 0; i < numberOfFilesToLoad;) {
				
				while(loadedFiles.size() >= numberOfMaxLoadedFiles) {
					final int upperDeleteBound = loadedFiles.size() / 2;
					final int filesToDeleteRandom = ThreadLocalRandom.current().nextInt(upperDeleteBound) + upperDeleteBound - 1;
					final int filesToDelete = Math.max(1, filesToDeleteRandom);
					
					System.out.println("Deleting " + filesToDelete + " files");
					
					for(int j = 0; j < filesToDelete; j++) {
						final int fileIdToDelete = ThreadLocalRandom.current().nextInt(loadedFiles.size());
						final String fileToDelete = loadedFiles.get(fileIdToDelete);
						deleteFile(fileToDelete);
					}
				}

				final int nextFileId = ThreadLocalRandom.current().nextInt(files.length);
				final String filename = files[nextFileId];
				boolean loaded = loadFile(filename);
				
				if(loaded) {
					i++;
				}
			}

			if(! nowait) {
				System.out.print("Please press enter to delete data: ");
				System.in.read();
			}

			// Delete all files before exit
			for(final String filename: loadedFiles) {
				deleteFile(filename);
			}
			loadedFiles.clear();

			System.out.println("Demo done");
			bboxDBCluster.close();
			bw.close();
			System.exit(0);

		} catch (InterruptedException | IOException e) {
			logger.error("Got exception while running demo class", e);
		}
	}

	/**
	 * Re-Create the distribution group and the table
	 */
	private void initBBoxDB() {
		try {
			// Delete old distribution group
			System.out.println("Delete old distribution group");
			final EmptyResultFuture dgroupDeleteResult = bboxDBCluster.deleteDistributionGroup(DGROUP);
			dgroupDeleteResult.waitForCompletion();

			if(dgroupDeleteResult.isFailed()) {
				System.err.println(dgroupDeleteResult.getAllMessages());
				System.exit(-1);
			}

			// Create new distribution group
			System.out.println("Create new distribution group, underflow " + underflowSize 
					+ " MB, overflow " + overflowSize + " MB");
			
			final DistributionGroupConfiguration dgroupConfig
				= DistributionGroupConfigurationBuilder
					.create(2)
					.withReplicationFactor((short) 1)
					.withMaximumRegionSizeInMB(overflowSize)
					.withMinimumRegionSizeInMB(underflowSize)
					.build();

			final EmptyResultFuture dgroupCreateResult = bboxDBCluster.createDistributionGroup(DGROUP, dgroupConfig);
			dgroupCreateResult.waitForCompletion();

			if(dgroupCreateResult.isFailed()) {
				System.err.println(dgroupCreateResult.getAllMessages());
				System.exit(-1);
			}

			// Create new table
			System.out.println("Create new table");
			final TupleStoreConfiguration storeConfiguration
				= TupleStoreConfigurationBuilder.create().allowDuplicates(false).build();

			final EmptyResultFuture tableCreateResult = bboxDBCluster.createTable(TABLE, storeConfiguration);
			tableCreateResult.waitForCompletion();

			if(tableCreateResult.isFailed()) {
				System.err.println(tableCreateResult.getAllMessages());
				System.exit(-1);
			}
			
			System.out.println("Files to process " + numberOfFilesToLoad);
			System.out.println("Max loaded files " + numberOfMaxLoadedFiles);
			
		} catch (Exception e) {
			System.err.println("Got an exception while prepating BBoxDB");
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Load the given file
	 * @param id
	 * @return
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	private boolean loadFile(final String filename) throws InterruptedException, IOException {

		if(loadedFiles.contains(filename)) {
			System.err.println("File " + filename + " is already loaded");
			return false;
		}
		
		if(processedFiles.contains(filename)) {
			return false;
		}

		System.out.print("Please press enter to load next file (loaded " 
				+ loadedFiles.size() + " / processed " + processedFiles.size() + ")");
		
		if(! nowait) {
			System.in.read();
		}
	
		final AtomicInteger lineNumber = new AtomicInteger(0);
		final String prefix = filename.hashCode() + "_";
		System.out.println("===> Loading content from: " + filename + " prefix is: " + prefix);


		try(final Stream<String> lines = Files.lines(Paths.get(filename))) {
			lines.forEach(l -> {
				final String key = prefix + lineNumber.getAndIncrement();
				final Tuple tuple = tupleBuilder.buildTuple(l, key);

				try {
					if(tuple != null) {
						final EmptyResultFuture insertFuture = bboxDBCluster.put(TABLE, tuple);
						pendingFutures.put(insertFuture);
					}
				} catch (BBoxDBException e) {
					logger.error("Got error while inserting tuple", e);
				}

				if(lineNumber.get() % 5000 == 0) {
					System.out.format("Loaded %d elements%n", lineNumber.get());
				}
			});
		} catch (IOException e) {
			System.err.println("Got an exeption while reading file: " + e);
			System.exit(-1);
		}

		pendingFutures.waitForCompletion();

		loadedFiles.add(filename);
		processedFiles.add(filename);

		System.out.println("Loaded content from: " + filename);

		return true;
	}

	/**
	 * Remove the given file
	 * @param fileid
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	private void deleteFile(final String filename) throws InterruptedException, IOException {

		if(! loadedFiles.contains(filename)) {
			System.err.println("File " + filename + " is not loaded");
			return;
		}
		
		if(! nowait) {
			System.out.print("Please press enter to delete file: " + filename);
			System.in.read();
		}
		
		final AtomicInteger lineNumber = new AtomicInteger(0);
		final String prefix = filename.hashCode() + "_";
		System.out.println("===> Removing content from: " + filename + " prefix is: " + prefix);


		try(final Stream<String> lines = Files.lines(Paths.get(filename))) {
			lines.forEach(l -> {
				final String key = prefix + lineNumber.getAndIncrement();
				try {
					final EmptyResultFuture resultFuture = bboxDBCluster.delete(TABLE, key);
					pendingFutures.put(resultFuture);

					if(lineNumber.get() % 5000 == 0) {
						System.out.format("Deleted %d elements%n", lineNumber.get());
					}
				} catch (BBoxDBException e) {
					logger.error("Got error while deleting tuple", e);
				}
			});
		} catch (IOException e) {
			System.err.println("Got an exeption while reading file: " + e);
			System.exit(-1);
		}

		pendingFutures.waitForCompletion();

		loadedFiles.remove(filename);
	}

	/**
	 * Are the files readable?
	 */
	private void checkFilesExist() {
		for(final String filename : files) {
			final File file = new File(filename);
			if(! file.exists()) {
				System.err.println("Unable to open file: " + filename);
				System.exit(-1);
			}
		}
	}

	/**
	 * Main Main Main Main Main Main
	 * @param args
	 */
	public static void main(final String[] args) {

		if(args.length < 7) {
			System.err.println("Usage: <Class> <File1>:<File2>:<FileN> <Number of files to load> <Max loaded files>"
					+ "<Underflow Size> <Overflow Size> <ZookeeperEndpoint> <Clustername>");
			System.exit(-1);
		}

		final String files = args[0];
		final String numberOfFilesToLoadString = args[1];
		final String numberOfMaxLoaddedFilesString = args[2];
		final String underflowStringSize = args[3];
		final String overflowStringSize = args[4];
		final String zookeeperEndpoint = args[5];
		final String clustername = args[6];
		final boolean nowait = args.length == 8;
		
		final BBoxDBCluster bboxDBCluster = new BBoxDBCluster(zookeeperEndpoint, clustername);
		bboxDBCluster.connect();

		if(! bboxDBCluster.isConnected()) {
			System.err.println("Unable to connect to zookeeper at: " + zookeeperEndpoint);
			System.exit(-1);
		}

		final int numberOfFilesToLoad = MathUtil.tryParseIntOrExit(numberOfFilesToLoadString,
				() -> "Unable to parse: " + numberOfFilesToLoadString);
		
		final int numberOfMaxLoadedFiles = MathUtil.tryParseIntOrExit(numberOfMaxLoaddedFilesString,
				() -> "Unable to parse: " + numberOfMaxLoaddedFilesString);
		
		final int underflowSize = MathUtil.tryParseIntOrExit(underflowStringSize, () -> "Unable to parse: " + underflowStringSize);
		final int overflowSize = MathUtil.tryParseIntOrExit(overflowStringSize, () -> "Unable to parse: " + overflowStringSize);

		final DataRedistributionLoader dataRedistributionLoader = new DataRedistributionLoader(files,
				numberOfFilesToLoad, numberOfMaxLoadedFiles, underflowSize, overflowSize, bboxDBCluster, nowait);

		dataRedistributionLoader.run();
	}
}

	
	