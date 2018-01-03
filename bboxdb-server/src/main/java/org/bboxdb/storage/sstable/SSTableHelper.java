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
package org.bboxdb.storage.sstable;

import java.io.File;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;

public class SSTableHelper {
	/**
	 * Extract the sequence Number from a given filename
	 * 
	 * @param Tablename, the name of the Table. Filename, the name of the file
	 * @return the sequence number
	 * @throws StorageManagerException 
	 */
	public static int extractSequenceFromFilename(final TupleStoreName tablename, final String filename)
			throws StorageManagerException {
		
		final String sequence = filename
				.replace(SSTableConst.SST_FILE_PREFIX, "")
				.replace(SSTableConst.SST_FILE_SUFFIX, "")
				.replace(SSTableConst.SST_INDEX_SUFFIX, "");
		
		try {
			return Integer.parseInt(sequence);
		} catch (NumberFormatException e) {
			final String error = "Unable to parse sequence number: " + filename + " / " + sequence;
			throw new StorageManagerException(error, e);
		}
	}
	
	/**
	 * Get the distribution group dir
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/2_dgroup1
	 */
	public static String getDistributionGroupDir(final String directory, final TupleStoreName name) {
		return getDistributionGroupDir(directory, name.getDistributionGroup());
	}
	
	/**
	 * Get the distribution group dir
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/2_dgroup1
	 */
	public static String getDistributionGroupDir(final String directory, final String name) {
		return getDataDir(directory)
				+ File.separator 
				+ name;
	}
	
	/**
	 * Get the data dir for a given volume dir
	 * @param directory
	 * @param tablename
	 * 
	 * @return e.g. /tmp/bboxdb/data/2_dgroup1
	 */
	public static String getDataDir(final String directory) {
		return directory 
				+ File.separator 
				+ "data";
	}
	
	/**
	 * Get the metadata file of a distribution group
	 */
	public static String getDistributionGroupMedatadaFile(final String directory, final String name) {
		return getDistributionGroupDir(directory, name)
				+ File.separator 
				+ SSTableConst.DISTRIBUTION_GROUP_MEDATADATA;
	}
	
	/**
	 * The full name of the SSTable directory for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/2_dgroup1/relation1(_regionid)
	 */
	public static String getSSTableDir(final String directory, final TupleStoreName name) {
		
		final StringBuilder regionSuffix = new StringBuilder();
		if(name.getRegionId() != TupleStoreName.INVALID_REGIONID) {
			regionSuffix.append("_");
			regionSuffix.append(name.getRegionId());
		}
		
		return getDistributionGroupDir(directory, name)
				+ File.separator
				+ name.getTablename()
				+ regionSuffix.toString();
	}
	
	/**
	 * The base name of the SSTable file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/2_dgroup1/relation1/sstable_relation1_2
	 */
	public static String getSSTableBase(final String directory, final TupleStoreName name, final int tablenumber) {
		return getSSTableDir(directory, name)
				+ File.separator 
				+ SSTableConst.SST_FILE_PREFIX 
				+ tablenumber;
	}
	
	/**
	 * The full name of the SSTable file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/relation1/sstable_relation1_2.sst
	 */
	public static String getSSTableFilename(final String directory, final TupleStoreName name, final int tablenumber) {
		return getSSTableBase(directory, name, tablenumber)
				+ SSTableConst.SST_FILE_SUFFIX;
	}
	
	/**
	 * The full name of the SSTable index file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/relation1/sstable_relation1_2.idx
	 */
	public static String getSSTableIndexFilename(final String directory, final TupleStoreName name, final int tablenumber) {
		return getSSTableBase(directory, name, tablenumber)
				+ SSTableConst.SST_INDEX_SUFFIX;
	}
	
	/**
	 * The full name of the SSTable bloom filter file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/relation1/sstable_relation1_2.blm
	 */
	public static String getSSTableBloomFilterFilename(final String directory, final TupleStoreName name, int tablebumber) {
		return getSSTableBase(directory, name, tablebumber)
				+ SSTableConst.SST_BLOOM_SUFFIX;
	}
	
	/**
	 * The full name of the spatial index file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/relation1/sstable_relation1_2.sidx
	 */
	public static String getSSTableSpatialIndexFilename(final String directory, final TupleStoreName name, final int tablenumber) {
		return getSSTableBase(directory, name, tablenumber)
				+ SSTableConst.SST_SPATIAL_INDEX_SUFFIX;
	}
	
	/**
	 * The full name of the SSTable metadata file for a given relation
	 * 
	 * @param directory
	 * @param name
	 * 
	 * @return e.g. /tmp/bboxdb/data/relation1/sstable_relation1_2.meta
	 */
	public static String getSSTableMetadataFilename(final String directory, final TupleStoreName name, final int tablenumber) {
		return getSSTableBase(directory, name, tablenumber)
				+ SSTableConst.SST_META_SUFFIX;
	}
	
	/**
	 * Belongs the given filename to a SSTable?
	 * 
	 * @param filename
	 * @return
	 */
	public static boolean isFileNameSSTable(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_FILE_SUFFIX);
	}
	
	/**
	 * Belongs the given filename to a SSTable index?
	 * 
	 * @param filename
	 * @return
	 */
	public static boolean isFileNameSSTableIndex(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_INDEX_SUFFIX);
	}
	
	/**
	 * Belongs the given filename to a SSTable bloom filter file?
	 * @param filename
	 * @return
	 */
	public static boolean isFileNameSSTableBloomFilter(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_BLOOM_SUFFIX);
	}
	
	/**
	 * Belongs the given filename to a spatial index file?
	 * @param filename
	 * @return
	 */
	public static boolean isFileNameSpatialIndex(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_SPATIAL_INDEX_SUFFIX);
	}

	/**
	 * Belongs the given filename to a SSTable meta file?
	 * @param filename
	 * @return
	 */
	public static boolean isFileNameSSTableMetadata(final String filename) {
		return filename.startsWith(SSTableConst.SST_FILE_PREFIX) 
				&& filename.endsWith(SSTableConst.SST_META_SUFFIX);
	}
	
	/**
	 * Belongs the given filename meta file?
	 * @param filename
	 * @return
	 */
	public static boolean isFileNameMetadata(final String filename) {
		return filename.endsWith(SSTableConst.SST_META_SUFFIX);
	}
}
