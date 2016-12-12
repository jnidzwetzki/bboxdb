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
package org.bboxdb.storage.sstable.reader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.BBoxDBService;
import org.bboxdb.storage.BloomFilterBuilder;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.SStableMetaData;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.predicate.Predicate;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;

public class SSTableFacade implements BBoxDBService, ReadOnlyTupleStorage {
	 
	/**
	 * The name of the table
	 */
	protected final SSTableName name;
	
	/**
	 * The Directoy for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The SStable reader
	 */
	protected final SSTableReader ssTableReader;
	
	/**
	 * The SSTable Key index reader
	 */
	protected final SSTableKeyIndexReader ssTableKeyIndexReader;
	
	/**
	 * The metadata of the sstable
	 */
	protected final SStableMetaData ssTableMetadata;
	
	/**
	 * The Bloom filter
	 */
	protected BloomFilter<String> bloomfilter;
	
	/**
	 * The number of the table
	 */
	protected final int tablenumber;
	
	/**
	 * The usage counter
	 */
	protected final AtomicInteger usage;
	
	/**
	 * Delete file on close
	 */
	protected volatile boolean deleteOnClose; 
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(SSTableFacade.class);
	

	public SSTableFacade(final String directory, final SSTableName tablename, final int tablenumber) throws StorageManagerException {
		super();
		this.name = tablename;
		this.directory = directory;
		this.tablenumber = tablenumber;
		
		ssTableReader = new SSTableReader(directory, tablename, tablenumber);
		ssTableKeyIndexReader = new SSTableKeyIndexReader(ssTableReader);
		
		final File bloomFilterFile = getBloomFilterFile(directory, tablename, tablenumber);
		loadBloomFilter(bloomFilterFile);
		
		final File metadataFile = getMetadataFile(directory, tablename, tablenumber);
		ssTableMetadata = SStableMetaData.importFromYamlFile(metadataFile);
		
		this.usage = new AtomicInteger(0);
		deleteOnClose = false;
	}
	
	protected void loadBloomFilter(final File bloomFilterFile) {
		
		if(! bloomFilterFile.exists()) {
			logger.warn("Bloom filter file {} does not exist, working without bloom filter");
			bloomfilter = null;
			return;
		}
	
		try {
			bloomfilter = BloomFilterBuilder.loadBloomFilterFromFile(bloomFilterFile);
		} catch (IOException e) {
			logger.warn("Unable to load the bloom filter", e);
			bloomfilter = null;
		}	
	}

	/**
	 * Calculate the name of the metadata file
	 * @param directory
	 * @param tablename
	 * @param tablenumber
	 * @return
	 */
	protected File getMetadataFile(final String directory,
			final SSTableName tablename, final int tablenumber) {
		final String metadatafile = SSTableHelper.getSSTableMetadataFilename(directory, tablename.getFullname(), tablenumber);
		return new File(metadatafile);
	}
	
	/**
	 * Get the name of the bloom filter file
	 * @param directory
	 * @param tablename
	 * @param tablenumber
	 * @return
	 */
	protected File getBloomFilterFile(final String directory,
			final SSTableName tablename, final int tablenumber) {
		final String bloomFilter = SSTableHelper.getSSTableBloomFilterFilename(directory, tablename.getFullname(), tablenumber);
		return new File(bloomFilter);
	}

	@Override
	public void init() {
		
		if(ssTableReader == null || ssTableKeyIndexReader == null) {
			logger.warn("init called but sstable reader or index reader is null");
			return;
		}
		
		ssTableReader.init();
		ssTableKeyIndexReader.init();
	}

	@Override
	public void shutdown() {
		
		if(ssTableReader == null || ssTableKeyIndexReader == null) {
			logger.warn("shutdown called but sstable reader or index reader is null");
			return;
		}
		
		ssTableKeyIndexReader.shutdown();
		ssTableReader.shutdown();
	}

	@Override
	public String getServicename() {
		return "SSTable facade for: " + name + " " + tablenumber;
	}

	@Override
	public String toString() {
		return "SSTableFacade [name=" + name.getFullname() + ", directory=" + directory
				+ ", tablenumber=" + tablenumber + ", oldestTupleTimestamp="
				+ getOldestTupleTimestamp() + ", newestTupleTimestamp="
				+ getNewestTupleTimestamp() + "]";
	}

	public SSTableReader getSsTableReader() {
		return ssTableReader;
	}

	public SSTableKeyIndexReader getSsTableKeyIndexReader() {
		return ssTableKeyIndexReader;
	}
	

	/* (non-Javadoc)
	 * @see org.bboxdb.storage.sstable.reader.Acquirable#deleteOnClose()
	 */
	@Override
	public void deleteOnClose() {
		deleteOnClose = true;
		testFileDelete();
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.storage.sstable.reader.Acquirable#acquire()
	 */
	@Override
	public boolean acquire() {
		
		// We are closing this instance
		if(deleteOnClose == true) {
			return false;
		}
		
		usage.incrementAndGet();
		return true;
	}
	
	/* (non-Javadoc)
	 * @see org.bboxdb.storage.sstable.reader.Acquirable#release()
	 */
	@Override
	public void release() {
		usage.decrementAndGet();
		
		testFileDelete();
	}

	/**
	 * Delete the file if possible
	 */
	protected void testFileDelete() {
		if(deleteOnClose && usage.get() == 0) {
			logger.info("Delete service facade for: {} / {}", name.getFullname(), tablenumber);
			
			shutdown();
			
			// Delete key index reader
			if(ssTableKeyIndexReader != null) {
				ssTableKeyIndexReader.delete();
			}
			
			// Delete reader
			if(ssTableReader != null) {
				ssTableReader.delete();
			}
			
			// Delete bloom filter
			final File bloomFilterFile = getBloomFilterFile(directory, name, tablenumber);
			bloomFilterFile.delete();
			
			// Delete metadata
			final File metadataFile = getMetadataFile(directory, name, tablenumber);
			metadataFile.delete();
		}
	}

	public String getName() {
		return name.getFullname();
	}

	public String getDirectory() {
		return directory;
	}

	public int getTablebumber() {
		return tablenumber;
	}

	public AtomicInteger getUsage() {
		return usage;
	}

	public SStableMetaData getSsTableMetadata() {
		return ssTableMetadata;
	}

	@Override
	public long getOldestTupleTimestamp() {
		return ssTableMetadata.getOldestTuple();
	}

	@Override
	public long getNewestTupleTimestamp() {
		return ssTableMetadata.getNewestTuple();
	}

	@Override
	public Tuple get(final String key) throws StorageManagerException {

		// Check bloom filter first
		if(bloomfilter == null) {
			logger.warn("File {} does not have a bloom filter", name);
		} else {
			if(! bloomfilter.mightContain(key)) {
				return null;
			}
		}
		
		final int position = ssTableKeyIndexReader.getPositionForTuple(key);
		
		// Does the tuple exist?
		if(position == -1) {
			return null;
		}
		
		return ssTableReader.getTupleAtPosition(position);
	}

	@Override
	public Iterator<Tuple> getMatchingTuples(final Predicate predicate) {
		return ssTableKeyIndexReader.getMatchingTuples(predicate);
	}

	@Override
	public Iterator<Tuple> iterator() {
		return ssTableKeyIndexReader.iterator();
	}

	@Override
	public long getNumberOfTuples() {
		return ssTableMetadata.getTuples();
	}

	@Override
	public Tuple getTupleAtPosition(final long position) throws StorageManagerException {
		try {
			return ssTableKeyIndexReader.getTupleForIndexEntry(position);
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}
	
}
