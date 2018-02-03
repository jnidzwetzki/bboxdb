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
package org.bboxdb.tools.generator;

import java.io.File;

import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.EmptyResultFuture;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;

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
	 * The name of the distribution group
	 */
	private final static String DGROUP = "demogroup";
	
	/**
	 * The name of the data table
	 */
	private final static String TABLE = DGROUP + "_osmtable";
	

	public DataRedistributionLoader(final String files, final BBoxDBCluster bboxDBCluster) {
		this.bboxDBCluster = bboxDBCluster;
		this.files = files.split(":");
	}

	/**
	 * Execute the loader
	 */
	@Override
	public void run() {
		checkFilesExist();
		initBBoxDB();
		
		while(true) {
			loadFile(1);
			deleteFile(1);
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
			dgroupDeleteResult.waitForAll();
			
			if(dgroupDeleteResult.isFailed()) {
				System.err.println(dgroupDeleteResult.getAllMessages());
				System.exit(-1);
			}
			
			// Create new distribution group
			System.out.println("Create new distribution group");
			final DistributionGroupConfiguration dgroupConfig 
				= DistributionGroupConfigurationBuilder.create(2).build();
			
			final EmptyResultFuture dgroupCreateResult = bboxDBCluster.createDistributionGroup(DGROUP, dgroupConfig);
			dgroupCreateResult.waitForAll();
			
			if(dgroupCreateResult.isFailed()) {
				System.err.println(dgroupCreateResult.getAllMessages());
				System.exit(-1);
			}
			
			// Create new table
			System.out.println("Create new table");
			final TupleStoreConfiguration storeConfiguration 
				= TupleStoreConfigurationBuilder.create().allowDuplicates(false).build();
			
			final EmptyResultFuture tableCreateResult = bboxDBCluster.createTable(TABLE, storeConfiguration);
			tableCreateResult.waitForAll();
			
			if(tableCreateResult.isFailed()) {
				System.err.println(tableCreateResult.getAllMessages());
				System.exit(-1);
			}
		} catch (Exception e) {
			System.err.println("Got an exception while prepating BBoxDB");
			e.printStackTrace();
			System.exit(-1);
		} 
	}

	/**
	 * Load the given file
	 * @param id
	 */
	private void loadFile(final int fileid) {
		final String filename = files[fileid];
		System.out.println("Loading content from: " + filename);
	}
	
	/**
	 * Remove the given file
	 * @param fileid
	 */
	private void deleteFile(final int fileid) {
		final String filename = files[fileid];
		System.out.println("Removing content from: " + filename);
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
		if(args.length != 3) {
			System.err.println("Usage: <Class> <File1>:<File2>:<FileN> <ZookeeperEndpoint> <Clustername>");
			System.exit(-1);
		}
		
		final String zookeeperEndpoint = args[1];
		final String clustername = args[2];
		
		final BBoxDBCluster bboxDBCluster = new BBoxDBCluster(zookeeperEndpoint, clustername);
		bboxDBCluster.connect();
		
		if(bboxDBCluster.isConnected()) {
			System.err.println("Unable to connect to zookeeper at: " + zookeeperEndpoint);
			System.exit(-1);
		}
		
		final DataRedistributionLoader dataRedistributionLoader = new DataRedistributionLoader(args[0], bboxDBCluster);
		dataRedistributionLoader.run();
	}
}
