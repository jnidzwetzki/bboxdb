/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.storage.sstable;

import java.io.File;

import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.DeletedTuple;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class SSTableHelper {
	/**
	 * Extract the sequence Number from a given filename
	 * 
	 * @param Tablename, the name of the Table. Filename, the name of the file
	 * @return the sequence number
	 * @throws StorageManagerException 
	 */
	public static int extractSequenceFromFilename(final SSTableName tablename, final String filename)
			throws StorageManagerException {
		try {
			final String sequence = filename
				.replace(SSTableConst.SST_FILE_PREFIX + tablename.getFullname() + "_", "")
				.replace(SSTableConst.SST_FILE_SUFFIX, "")
				.replace(SSTableConst.SST_INDEX_SUFFIX, "");
			
			return Integer.parseInt(sequence);
		
		} catch (NumberFormatException e) {
			String error = "Unable to parse sequence number: " + filename;
			throw new StorageManagerException(error, e);
		}
	}
	
	/**
	 * The full name of the SSTable directory for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1 
	 */
	public static String getSSTableDir(final String directory, final String name) {
		return directory 
				+ File.separator 
				+ name;
	}
	
	/**
	 * The base name of the SSTable file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2
	 */
	public static String getSSTableBase(final String directory, final String name, int tablebumber) {
		return getSSTableDir(directory, name)
				+ File.separator 
				+ SSTableConst.SST_FILE_PREFIX 
				+ name 
				+ "_" 
				+ tablebumber;
	}
	
	/**
	 * The full name of the SSTable file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2.sst
	 */
	public static String getSSTableFilename(final String directory, final String name, int tablebumber) {
		return getSSTableBase(directory, name, tablebumber)
				+ SSTableConst.SST_FILE_SUFFIX;
	}
	
	/**
	 * The full name of the SSTable index file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2.idx
	 */
	public static String getSSTableIndexFilename(final String directory, final String name, int tablebumber) {
		return getSSTableBase(directory, name, tablebumber)
				+ SSTableConst.SST_INDEX_SUFFIX;
	}
	
	/**
	 * The full name of the SSTable metadata file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/scalephant/data/relation1/sstable_relation1_2.meta
	 */
	public static String getSSTableMetadataFilename(final String directory, final String name, int tablebumber) {
		return getSSTableBase(directory, name, tablebumber)
				+ SSTableConst.SST_META_SUFFIX;
	}
	
	/**
	 * If the tuple is a deleted tuple, return null
	 * Otherwise, return the given tuple
	 * @param tuple
	 * @return
	 */
	public static Tuple replaceDeletedTupleWithNull(final Tuple tuple) {
		
		if(tuple == null) {
			return null;
		}
		
		if(tuple instanceof DeletedTuple) {
			return null;
		}
		
		return tuple;
	}
	
	/**
	 * Return the most recent version of the tuple
	 * @param tuple1
	 * @param tuple2
	 * @return
	 */
	public static Tuple returnMostRecentTuple(final Tuple tuple1, final Tuple tuple2) {
		if(tuple1 == null && tuple2 == null) {
			return null;
		}
		
		if(tuple1 == null) {
			return tuple2;
		}
		
		if(tuple2 == null) {
			return tuple1;
		}
		
		if(tuple1.getTimestamp() > tuple2.getTimestamp()) {
			return tuple1;
		}
		
		return tuple2;
	}
}
