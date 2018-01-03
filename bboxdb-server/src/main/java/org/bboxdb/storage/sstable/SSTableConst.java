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

import java.util.concurrent.TimeUnit;

public class SSTableConst {
	
	/**
	 * The magic bytes at the beginning of every SSTable file
	 */
	public final static byte[] MAGIC_BYTES = "bboxdb".getBytes();
	
	/**
	 * The magic bytes at the beginning of every SSTable index file
	 */
	public final static byte[] MAGIC_BYTES_INDEX = "bboxdb-idx".getBytes();
	
	/**
	 * The magic bytes at the beginning of every spatial index file
	 */
	public final static byte[] MAGIC_BYTES_SPATIAL_RTREE_INDEX = "bboxdb-sidx".getBytes();
	
	/**
	 * The current version of the SSTable layout format
	 */
	public final short SST_VERSION = 1;
	
	/**
	 * The prefix for every SSTable file
	 */
	public final static String SST_FILE_PREFIX = "sstable_";
	
	/**
	 * The suffix for every SSTable file
	 */
	public final static String SST_FILE_SUFFIX = ".sst";
	
	/**
	 * The suffix for every SSTable index file
	 */
	public final static String SST_INDEX_SUFFIX = ".idx";
	
	/**
	 * The suffix for every spatial index file
	 */
	public final static String SST_SPATIAL_INDEX_SUFFIX = ".sidx";

	/**
	 * The suffix for persistent bloom filters
	 */
	public final static String SST_BLOOM_SUFFIX = ".blm";
	
	/**
	 * The suffix for the meta files
	 */
	public final static String SST_META_SUFFIX = ".meta";
	
	/**
	 * Distribution group medata data file
	 */
	public static final String DISTRIBUTION_GROUP_MEDATADATA = "group.meta";

	/**
	 * Tuple store metadata file
	 */
	public static final String TUPLE_STORE_METADATA = "tuplestore.meta";

	/**
	 * Format of the index file:
	 * 
	 * -------------------------------------------------
	 * | Tuple-Position | Tuple-Position |  .........  |
	 * |     4 Byte     |     4 Byte     |  .........  |
	 * -------------------------------------------------
	 */
	public final static int INDEX_ENTRY_BYTES = 4;
	
	/**
	 * Marker for deleted tuples
	 */
	public final static byte[] DELETED_MARKER = "DEL".getBytes();
	
	/**
	 * Execution interval for the compact thread (30 seconds)
	 */
	public final static long COMPACT_THREAD_DELAY = TimeUnit.SECONDS.toMillis(30);
	
	/**
	 * Execution interval for the checkpoint thread
	 */
	public final static long CHECKPOINT_THREAD_DELAY = TimeUnit.SECONDS.toMillis(60);
	
	/**
	 * The maximal size for one SSTable. SStables are mapped into memory, the JVM can only
	 * map files up to 2 GB. Therefore the limit is set to 1.9 GB.
	 */
	public final static int MAX_SSTABLE_SIZE = 2040109465;
	
	/**
	 * The maximal amount of unflushed memtables per SSTable
	 */
	public final static int MAX_UNFLUSHED_MEMTABLES_PER_TABLE = 20;
	
	/**
	 * Elements in key cache
	 */
	public final static int KEY_CACHE_ELEMENTS = 1000;
}
