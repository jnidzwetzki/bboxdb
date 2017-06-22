/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.network.client.BBoxDBException;
import org.bboxdb.storage.BloomFilterBuilder;
import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.SSTableMetaData;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.sstable.SSTableHelper;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndex;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.BloomFilter;

public class SSTableFacade implements BBoxDBService, ReadOnlyTupleStorage {
	 
	/**
	 * The name of the table
	 */
	protected final SSTableName tablename;
	
	/**
	 * The Directory for the SSTables
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
	protected final SSTableMetaData ssTableMetadata;
	
	/**
	 * The spatial index
	 */
	protected SpatialIndex spatialIndex;
	
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
		this.tablename = tablename;
		this.directory = directory;
		this.tablenumber = tablenumber;
		
		ssTableReader = new SSTableReader(directory, tablename, tablenumber);
		ssTableKeyIndexReader = new SSTableKeyIndexReader(ssTableReader);
		
		// Meta data
		final File metadataFile = getMetadataFile(directory, tablename, tablenumber);
		ssTableMetadata = SSTableMetaData.importFromYamlFile(metadataFile);
		
		this.usage = new AtomicInteger(0);
		deleteOnClose = false;
	}

	/**
	 * Get the spatial index file
	 * @param directory
	 * @param tablename
	 * @param tablenumber
	 * @return
	 */
	protected File getSpatialIndexFile(final String directory, final SSTableName tablename, final int tablenumber) {
		final String spatialIndexFileName = SSTableHelper.getSSTableSpatialIndexFilename(directory, tablename, tablenumber);
		final File spatialIndexFile = new File(spatialIndexFileName);
		return spatialIndexFile;
	}

	/**
	 * Get the bloomfilter file
	 * @param directory
	 * @param tablename
	 * @param tablenumber
	 * @return
	 */
	protected File getBloomFilterFile(final String directory, final SSTableName tablename, final int tablenumber) {
		final String bloomFilterFileName = SSTableHelper.getSSTableBloomFilterFilename(directory, tablename, tablenumber);
		final File bloomFilterFile = new File(bloomFilterFileName);
		return bloomFilterFile;
	}
	
	/**
	 * Load the spatial index from file
	 * @throws StorageManagerException 
	 */
	protected void loadSpatialIndex(final File spatialIndexFile) throws StorageManagerException {
		if(! spatialIndexFile.exists()) {
			throw new StorageManagerException("The spatial index does not exists: " + spatialIndexFile);
		}
		
		try (    final FileInputStream fis = new FileInputStream(spatialIndexFile);
				 final BufferedInputStream inputStream = new BufferedInputStream(fis);) {
			
			spatialIndex = SpatialIndexFactory.getInstance();
			spatialIndex.readFromStream(inputStream);
			
		} catch (Exception e) {
			throw new StorageManagerException(e);
		}
	}
	
	/**
	 * Load the boom filter from file
	 * @param bloomFilterFile
	 */
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
		final String metadatafile = SSTableHelper.getSSTableMetadataFilename(directory, tablename, tablenumber);
		return new File(metadatafile);
	}
	
	@Override
	public void init() throws InterruptedException, BBoxDBException {
		
		try {
			if(ssTableReader == null || ssTableKeyIndexReader == null) {
				logger.warn("init called but sstable reader or index reader is null");
				return;
			}
			
			ssTableReader.init();
			ssTableKeyIndexReader.init();
			
			// Spatial index
			final File spatialIndexFile = getSpatialIndexFile(directory, tablename, tablenumber);
			loadSpatialIndex(spatialIndexFile); 
			
			// Bloom filter
			final File bloomFilterFile = getBloomFilterFile(directory, tablename, tablenumber);
			loadBloomFilter(bloomFilterFile);
		} catch (StorageManagerException e) {
			throw new BBoxDBException(e);
		}
	}

	@Override
	public void shutdown() {
		if(ssTableKeyIndexReader != null) {
			ssTableKeyIndexReader.shutdown();
		}
		
		if(ssTableReader != null) {
			ssTableReader.shutdown();
		}
	}

	@Override
	public String getServicename() {
		return "SSTable facade for: " + tablename + " " + tablenumber;
	}

	@Override
	public String toString() {
		return "SSTableFacade [name=" + tablename.getFullname() + ", directory=" + directory
				+ ", tablenumber=" + tablenumber + ", oldestTupleTimestamp="
				+ getOldestTupleVersionTimestamp() + ", newestTupleTimestamp="
				+ getNewestTupleVersionTimestamp() + ", deleteOnClose=" + deleteOnClose + "]";
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
		
		assert (usage.get() > 0);
		
		usage.decrementAndGet();
		
		testFileDelete();
	}

	/**
	 * Delete the file if possible
	 */
	protected void testFileDelete() {
		if(deleteOnClose && usage.get() == 0) {
			logger.info("Delete service facade for: {} / {}", tablename.getFullname(), tablenumber);
			
			shutdown();
			
			// Delete key index reader
			if(ssTableKeyIndexReader != null) {
				ssTableKeyIndexReader.delete();
			}
			
			// Delete reader
			if(ssTableReader != null) {
				ssTableReader.delete();
			}
			
			// Delete spatial index
			final File spatialIndexFile = getSpatialIndexFile(directory, tablename, tablenumber);
			spatialIndexFile.delete();
			
			// Delete bloom filter
			final File bloomFilterFile = getBloomFilterFile(directory, tablename, tablenumber);
			bloomFilterFile.delete();
			
			// Delete metadata
			final File metadataFile = getMetadataFile(directory, tablename, tablenumber);
			metadataFile.delete();
		}
	}

	@Override
	public String getInternalName() {
		return tablename.getFullname() + " / " + tablenumber;
	}

	@Override
	public SSTableName getSStableName() {
		return tablename;
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

	public SSTableMetaData getSsTableMetadata() {
		return ssTableMetadata;
	}

	@Override
	public long getOldestTupleVersionTimestamp() {
		return ssTableMetadata.getOldestTupleVersionTimestamp();
	}

	@Override
	public long getNewestTupleVersionTimestamp() {
		return ssTableMetadata.getNewestTupleVersionTimestamp();
	}
	
	@Override
	public long getNewestTupleInsertedTimestamp() {
		return ssTableMetadata.getNewestTupleInsertedTimstamp();
	}

	@Override
	public Tuple get(final String key) throws StorageManagerException {
		
		assert (usage.get() > 0);

		// Check bloom filter first
		if(bloomfilter == null) {
			logger.warn("File {} does not have a bloom filter", tablename);
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
	public Iterator<Tuple> iterator() {
		
		assert (usage.get() > 0);

		return ssTableKeyIndexReader.iterator();
	}

	@Override
	public long getNumberOfTuples() {

		assert (usage.get() > 0);
		
		return ssTableMetadata.getTuples();
	}

	@Override
	public Tuple getTupleAtPosition(final long position) throws StorageManagerException {
		
		assert (usage.get() > 0);
		
		try {
			return ssTableKeyIndexReader.getTupleForIndexEntry(position);
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}
	
	/**
	 * Get the size on disk of the facade 
	 * @return
	 */
	public long getSize() {
		return ssTableKeyIndexReader.getSize() + ssTableReader.getSize();
	}

	@Override
	public Iterator<Tuple> getAllTuplesInBoundingBox(final BoundingBox boundingBox) {
		assert (usage.get() > 0);

		List<? extends SpatialIndexEntry> entries = spatialIndex.getEntriesForRegion(boundingBox);
		@SuppressWarnings("rawtypes")
		final Iterator entryIterator = entries.iterator();
		
		return new Iterator<Tuple>() {

			@Override
			public boolean hasNext() {
				return entryIterator.hasNext();
			}

			@Override
			public Tuple next() {
				final SpatialIndexEntry entry = (SpatialIndexEntry) entryIterator.next();
				final String key = entry.getKey();
				
				try {
					return get(key);
				} catch (StorageManagerException e) {
					throw new RuntimeException(e);
				}
			}
		};
	}

	@Override
	public boolean isPersistent() {
		return true;
	}
	
	@Override
	public boolean isDeletePending() {
		return deleteOnClose;
	}
	
}
