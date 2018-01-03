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
package org.bboxdb.storage.tuplestore;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.distribution.DistributionGroupName;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableHelper;

public class TupleStoreLocator {

	/**
	 * Scan the given directory for existing sstables
	 * @param storageDirectory
	 * @return 
	 * @throws StorageManagerException 
	 */
	public static Map<TupleStoreName, String> scanDirectoryForExistingTables(final String storageDirectory) 
			throws StorageManagerException {
	
		final String dataDirString = SSTableHelper.getDataDir(storageDirectory);
		final File dataDir = new File(dataDirString);
		
		if(! dataDir.exists()) {
			throw new StorageManagerException("Root dir does not exist: " + dataDir);
		}
		
		final Map<TupleStoreName, String> sstableLocations = new HashMap<>();

		// Distribution groups
		for (final File fileEntry : dataDir.listFiles()) {
			
	        if (! fileEntry.isDirectory()) {
	        	continue;
	        }
	        
        	final String distributionGroup = fileEntry.getName();
        	final DistributionGroupName distributionGroupName = new DistributionGroupName(distributionGroup);
        	
        	assert(distributionGroupName.isValid()) : "Invalid name: " + distributionGroup;
        	
        	final Map<TupleStoreName, String> tablesInDir 
        		= handleDistributionGroupEntry(storageDirectory, fileEntry, distributionGroupName);
        	
        	sstableLocations.putAll(tablesInDir);
	    }
		
		return sstableLocations;
	}

	/**
	 * Handle the subdirs of the distribution group
	 * @param storageDirectory
	 * @param fileEntry
	 * @param distributionGroupName
	 * @return
	 */
	protected static Map<TupleStoreName, String> handleDistributionGroupEntry(final String storageDirectory,
			final File fileEntry, final DistributionGroupName distributionGroupName) {
		
		final Map<TupleStoreName, String> sstableLocations = new HashMap<>();
		
		// Tables
		for (final File tableEntry : fileEntry.listFiles()) {
		    if (tableEntry.isDirectory()) {
		    	final String tablename = tableEntry.getName();
		    	final String fullname = distributionGroupName.getFullname() + "_" + tablename;
		    	final TupleStoreName sstableName = new TupleStoreName(fullname);
				sstableLocations.put(sstableName, storageDirectory);
		    }
		}
		
		return sstableLocations;
	}

}
