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
package org.bboxdb.storage.sstable.reader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.bboxdb.commons.io.UnsafeMemoryHelper;
import org.bboxdb.misc.BBoxDBService;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTableReader implements BBoxDBService {

	/**
	 * The number of the table
	 */
	protected final int tablebumber;
	
	/**
	 * The name of the table
	 */
	protected final TupleStoreName name;
	
	/**
	 * The filename of the table
	 */
	protected File file;
	
	/**
	 * The Directory for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The memory region
	 */
	protected MappedByteBuffer memory;

	/**
	 * The file to read
	 */
	protected RandomAccessFile randomAccessFile;
	
	/**
	 * The corresponding fileChanel
	 */
	protected FileChannel fileChannel;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(AbstractTableReader.class);
	
	public AbstractTableReader(final String directory, final TupleStoreName name, final int tablenumer) throws StorageManagerException {
		this.name = name;
		this.directory = directory;
		this.tablebumber = tablenumer;
		this.file = constructFileToRead();
		
		if(! UnsafeMemoryHelper.isDirectMemoryUnmapperAvailable()) {
			logger.error("Memory unmapper not available, please use a oracle JVM");
			System.exit(-1);
		}
	}
	
	/**
	 * Construct the filename to read
	 * 
	 * @return
	 */
	protected abstract File constructFileToRead();
	
	/**
	 * Get the sequence number of the SSTable
	 * 
	 * @return
	 */
	public int getTablebumber() {
		return tablebumber;
	}


	/**
	 * Open a stored SSTable and read the magic bytes
	 * 
	 * @return a InputStream or null
	 * @throws StorageManagerException
	 */
	protected void validateFile() throws StorageManagerException {
		
		final byte[] expectedMagicBytes = getMagicBytes();
		
		// Validate file - read the magic from the beginning
		final byte[] magicBytes = new byte[expectedMagicBytes.length];
		
		memory.get(magicBytes, 0, expectedMagicBytes.length);

		if(! Arrays.equals(magicBytes, expectedMagicBytes)) {
			throw new StorageManagerException("File " + file + " does not contain the magic bytes");
		}
	}
	
	/**
	 * Get the magic bytes for the file
	 * @return
	 */
	protected abstract byte[] getMagicBytes();
	
	/**
	 * Reset the position to the first element
	 */
	protected void resetPosition() {
		
		final byte[] magicBytes = getMagicBytes();
		
		memory.position(magicBytes.length);
	}

	/**
	 * Init the resources
	 * 
	 * The file channel resource is closed in the shutdown method
	 */
	@Override
	public void init() {
		try {
			randomAccessFile = new RandomAccessFile(file, "r");
			fileChannel = randomAccessFile.getChannel();
			memory = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
			memory.order(Const.APPLICATION_BYTE_ORDER);
			validateFile();
		} catch (Exception e) {
			if(! Thread.currentThread().isInterrupted()) {
				logger.error("Error during an IO operation", e);
			}
			shutdown();
		} 
	}

	@Override
	public void shutdown() {
	
		if(fileChannel != null) {
			try {
				fileChannel.close();
				fileChannel = null;
			} catch (IOException e) {
				if(! Thread.currentThread().isInterrupted()) {
					logger.error("Error during an IO operation", e);
				}
			}
		}
		
		if(randomAccessFile != null) {
			try {
				randomAccessFile.close();
			} catch (IOException e) {
				if(! Thread.currentThread().isInterrupted()) {
					logger.error("Error during an IO operation", e);
				}
			}
			randomAccessFile = null;
		}
		
		if(memory != null) {
			UnsafeMemoryHelper.unmapMemory(memory);
			memory = null;
		}
	}
	
	/**
	 * Is the reader ready?
	 */
	protected boolean isReady() {
		return memory != null;
	}

	/**
	 * Get the name
	 * @return the file handle
	 */
	public TupleStoreName getName() {
		return name;
	}

	/**
	 * Get the file handle
	 * @return
	 */
	public File getFile() {
		return file;
	}

	/**
	 * Get the directory
	 * @return
	 */
	public String getDirectory() {
		return directory;
	}

	/**
	 * Delete the file
	 */
	public void delete() {
		shutdown();

		synchronized (this) {
			if(file != null) {
				logger.debug("Delete file: {}", file);
				file.delete();
				file = null;
			}
		}
	}
	
	/**
	 * Get the size of the file
	 * @return
	 */
	public long getSize() {
		return file.length();
	}
	
	/**
	 * Get the last modified timestamp
	 * @return 
	 */
	public long getLastModifiedTimestamp() {
		return file.lastModified();
	}
	
	/**
	 * Get the memory buffer
	 * @return
	 */
	public MappedByteBuffer getMemory() {
		return memory;
	}
}
