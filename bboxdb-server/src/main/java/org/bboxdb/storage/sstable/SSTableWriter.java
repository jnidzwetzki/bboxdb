/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.storage.BloomFilterBuilder;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreMetaData;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilderFactory;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.BloomFilter;
import com.google.common.io.CountingOutputStream;

import io.prometheus.client.Counter;

public class SSTableWriter implements AutoCloseable {
	
	/**
	 * The number of the table
	 */
	private final int tablenumber;
	
	/**
	 * The name of the table
	 */
	private final TupleStoreName name;
	
	/**
	 * The directory for the SSTables
	 */
	private final String directory;
	
	/**
	 * SSTable output stream
	 */
	private CountingOutputStream sstableOutputStream;
	
	/**
	 * SSTable index stream
	 */
	private OutputStream sstableIndexOutputStream;
	
	/**
	 * The SSTable file object
	 */
	private File sstableFile;
	
	/**
	 * The SSTable index file object
	 */
	private File sstableIndexFile;
	
	/**
	 * The bloom filter file
	 */
	private File sstableBloomFilterFile;
	
	/**
	 * The spatial index file
	 */
	private File spatialIndexFile;
	
	/**
	 * The meta data file
	 */
	private File metadataFile;
	
	/**
	 * A counter for the written tuples
	 */
	private final SSTableMetadataBuilder metadataBuilder;
	
	/**
	 * The bloom filter
	 */
	private final BloomFilter<String> bloomFilter;
	
	/**
	 * The spatial index
	 */
	private final SpatialIndexBuilder spatialIndex;
	
	/**
	 * The error flag
	 */
	private boolean exceptionDuringWrite;

	/**
	 * The amount of written tuple bytes
	 */
	private final static Counter writtenTuplesBytes = Counter.build()
			.name("bboxdb_written_tuple_bytes")
			.help("Written tuple bytes")
			.register();
	
	/**
	 * The amount of written tuples
	 */
	private final static Counter writtenTuplesTotal = Counter.build()
			.name("bboxdb_written_tuple_total")
			.help("Written tuples total")
			.register();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

	
	public SSTableWriter(final String directory, final TupleStoreName name, 
			final int tablenumber, final long estimatedNumberOfTuples, final SSTableCreator creator) {
		
		this.directory = directory;
		this.name = name;
		this.tablenumber = tablenumber;
		this.metadataBuilder = new SSTableMetadataBuilder(creator);
		this.exceptionDuringWrite = false;
		
		// Bloom Filter
		final String sstableBloomFilterFilename = SSTableHelper.getSSTableBloomFilterFilename(directory, name, tablenumber);
		this.sstableBloomFilterFile = new File(sstableBloomFilterFilename);
		this.bloomFilter = BloomFilterBuilder.buildBloomFilter(estimatedNumberOfTuples);
		
		// Spatial index
		final String spatialIndexFilename =  SSTableHelper.getSSTableSpatialIndexFilename(directory, name, tablenumber);
		this.spatialIndexFile = new File(spatialIndexFilename);
		this.spatialIndex = SpatialIndexBuilderFactory.getInstance();
		
		// Metadata
		final String ssTableMetadataFilename = SSTableHelper.getSSTableMetadataFilename(directory, name, tablenumber);
		this.metadataFile = new File(ssTableMetadataFilename);
	}
	
	/**
	 * Open all required files
	 * @throws StorageManagerException
	 */
	public void open() throws StorageManagerException {
		final String directoryName = SSTableHelper.getSSTableDir(directory, name);
		final File directoryHandle = new File(directoryName);
		
		if(! directoryHandle.isDirectory()) {
			final String error = "Directory for SSTable " + name + " does not exist: " + directoryName;
			logger.error(error);
			throw new StorageManagerException(error);
		}
		
		final String sstableOutputFileName = SSTableHelper.getSSTableFilename(directory, name, tablenumber);
		sstableFile = new File(sstableOutputFileName);
		
		final String outputIndexFileName = SSTableHelper.getSSTableIndexFilename(directory, name, tablenumber);
		sstableIndexFile = new File(outputIndexFileName);
		
		// Don't overwrite old data
		if(sstableFile.exists()) {
			throw new StorageManagerException("Table file already exists: " + sstableOutputFileName);
		}
		
		if(sstableIndexFile.exists()) {
			throw new StorageManagerException("Table file already exists: " + sstableIndexFile);
		}
		
		if(sstableBloomFilterFile.exists()) {
			throw new StorageManagerException("Bloom filter file already exists: " + sstableBloomFilterFile);
		}
		
		try {
			logger.debug("Writing new SSTable for relation: {} file: {}", name.getFullname(), sstableOutputFileName);
			final BufferedOutputStream sstableFileOutputStream = new BufferedOutputStream(new FileOutputStream(sstableFile));
			sstableOutputStream = new CountingOutputStream(sstableFileOutputStream);
			sstableOutputStream.write(SSTableConst.MAGIC_BYTES_SSTABLE);
			
			sstableIndexOutputStream = new BufferedOutputStream(new FileOutputStream(sstableIndexFile));
			sstableIndexOutputStream.write(SSTableConst.MAGIC_BYTES_INDEX);
		} catch (FileNotFoundException e) {
			exceptionDuringWrite = true;
			throw new StorageManagerException("Unable to open output file", e);
		} catch (IOException e) {
			exceptionDuringWrite = true;
			throw new StorageManagerException("Unable to write into output file", e);
		}
	}
	
	/**
	 * Close all open file handles and write the meta data
	 */
	public void close() throws StorageManagerException {
		try {			
			logger.debug("Closing new written SSTable for relation: {} number {}. File: {} ", 
					name.getFullname(), tablenumber, sstableFile.getName());

			if(sstableOutputStream != null) {
				sstableOutputStream.close();
				sstableOutputStream = null;
			}
			
			if(sstableIndexOutputStream != null) {
				sstableIndexOutputStream.close();
				sstableIndexOutputStream = null;
			}
			
			writeSpatialIndex();
			writeBloomFilter();
			writeMetadata();
			
		} catch (IOException e) {
			exceptionDuringWrite = true;
			throw new StorageManagerException("Exception while closing streams", e);
		} finally {
			checkForWriteException();
		}
	}

	/**
	 *  Delete half written files if an exception has occurred
	 *  The variable exceptionDuringWrite is set to true in every catch block
	 */
	private void checkForWriteException() {
		if(exceptionDuringWrite == true) {
			deleteFromDisk();
		}
	}
	
	/**
	 * Delete written data from disk
	 */
	public void deleteFromDisk() {
		final File filesArray[] = {sstableFile, sstableIndexFile, sstableBloomFilterFile, 
				spatialIndexFile, metadataFile};
		
		final List<File> filesToDelete = Arrays.asList(filesArray);
		
		for(final File file : filesToDelete) {
			if(file != null && file.exists()) {
				file.delete();
			}
		}
	}
	
	/**
	 * Write the spatial index to file
	 * @throws IOException
	 * @throws StorageManagerException 
	 */
	private void writeSpatialIndex() throws IOException, StorageManagerException {
		try (   
				final RandomAccessFile file = new RandomAccessFile(spatialIndexFile, "rw" );
			) {
			spatialIndex.writeToFile(file);
			file.close();
		}
	}
	
	/**
	 * Write the bloom filter into the filter file
	 * @throws IOException
	 */
	private void writeBloomFilter() throws IOException {
		
		try (   final FileOutputStream fos = new FileOutputStream(sstableBloomFilterFile);
				final OutputStream outputStream = new BufferedOutputStream(fos);
			) {
			
			bloomFilter.writeTo(outputStream);
			outputStream.close();
		}
	}
	
	/**
	 * Write the meta data to yaml info file
	 * @throws IOException
	 */
	private void writeMetadata() throws IOException {
		final TupleStoreMetaData metadata = metadataBuilder.getMetaData();
		metadata.exportToYamlFile(metadataFile);
	}
	
	/**
	 * Set the error flag
	 */
	@VisibleForTesting
	public void setErrorFlag() {
		exceptionDuringWrite = true;
	}
	
	/**
	 * Add the next tuple into the result sstable
	 * @param tuple
	 * @throws IOException
	 * @throws StorageManagerException 
	 */
	public void addTuple(final Tuple tuple) throws StorageManagerException {
		final int tuplePosition = addTupleWithoutSpatialIndex(tuple);
		
		// Don't add deleted tuples to the index
		if(tuple instanceof DeletedTuple) {
			return;
		}
		
		// Add tuple to the spatial index
		final SpatialIndexEntry sIndexentry 
			= new SpatialIndexEntry(tuple.getBoundingBox(), tuplePosition);
		
		spatialIndex.insert(sIndexentry);
	}
	
	/**
	 * Add a list of tuples
	 * @param tuples
	 * @throws StorageManagerException
	 */
	public void addTuples(final Collection<Tuple> tuples) throws StorageManagerException {
		
		assert(sstableOutputStream != null) : "The output stream has to be open";
			
		try {
			for(final Tuple tuple : tuples) {
				addTuple(tuple);
			}
			
		} catch(StorageManagerException e) {
			exceptionDuringWrite = true;
			throw e;
		}
	}

	/**
	 * Write the tuple without building the spatial index 
	 * (e.g., for writing pre indexed data) 
	 * @param tuple
	 * @return
	 * @throws StorageManagerException
	 */
	public int addTupleWithoutSpatialIndex(final Tuple tuple) throws StorageManagerException {
		try {
			// Add Tuple to the index
			final int tuplePosition = (int) sstableOutputStream.getCount();
			writeIndexEntry(tuplePosition);
			
			final int newPosition = (int) sstableOutputStream.getCount();
			final int writtenBytes = newPosition - tuplePosition;
			
			// Add Tuple to the SSTable file
			TupleHelper.writeTupleToStream(tuple, sstableOutputStream);
			metadataBuilder.updateWithTuple(tuple);
			
			// Add tuple to the bloom filter
			bloomFilter.put(tuple.getKey());
			
			writtenTuplesTotal.inc();
			writtenTuplesBytes.inc(writtenBytes);
			
			return tuplePosition;
		} catch (IOException e) {
			exceptionDuringWrite = true;
			throw new StorageManagerException("Unable to write tuple to SSTable", e);
		}
	}

	/** 
	 * Append an entry to the index file.
	 * 
	 * Format of the index file:
	 * 
	 * -------------------------------------------------
	 * | Tuple-Position | Tuple-Position |  .........  |
 	 * |     4 Byte     |     4 Byte     |  .........  |
	 * -------------------------------------------------
	 * 
	 * @param keyLengthBytes
	 * @param keyPosition
	 * @throws IOException
	 */
	private void writeIndexEntry(final int tuplePosition) throws IOException {
		final ByteBuffer tuplePositionBytes = DataEncoderHelper.intToByteBuffer(tuplePosition);
		sstableIndexOutputStream.write(tuplePositionBytes.array());
	}

	/**
	 * Get the sstable output file
	 * @return
	 */
	public File getSstableFile() {
		return sstableFile;
	}

	/**
	 * Get the sstable index output file
	 * @return
	 */
	public File getSstableIndexFile() {
		return sstableIndexFile;
	}
	
	/**
	 * Get the metadata file
	 * @return
	 */
	public File getMetadataFile() {
		return metadataFile;
	}
	
	/**
	 * Does an exception during a write operation occurred?
	 * @return
	 */
	public boolean isErrorFlagSet() {
		return exceptionDuringWrite;
	}
	
	/**
	 * Get the already written bytes for this SSTable
	 * @return
	 */
	public long getWrittenBytes() {
		return sstableOutputStream.getCount();
	}
	
	/**
	 * Get the sstable name
	 */
	public TupleStoreName getName() {
		return name;
	}
	
	/**
	 * Get the tablenumber
	 * @return
	 */
	public int getTablenumber() {
		return tablenumber;
	}
	
	/**
	 * Get the directory
	 * @return
	 */
	public String getDirectory() {
		return directory;
	}
	
}
