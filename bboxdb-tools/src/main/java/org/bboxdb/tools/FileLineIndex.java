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
package org.bboxdb.tools;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

import org.apache.commons.compress.utils.CountingInputStream;
import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.commons.io.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class FileLineIndex implements AutoCloseable {

	/**
	 * The file to index
	 */
	protected final String filename;
	
	/**
	 * The database dir
	 */
	protected Path tmpDatabaseDir = null;
	
	/**
	 * The database handle
	 */
	private Database database = null;

	/**
	 * The database environment
	 */
	private Environment dbEnv = null;
	
	/**
	 * The indexed lines
	 */
	protected long indexedLines = 0;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(FileLineIndex.class);


	public FileLineIndex(final String filename) {
		this.filename = Objects.requireNonNull(filename);
	}
	
	/**
	 * Index the given file
	 * @throws IOException
	 */
	public void indexFile() throws IOException {
		final File file = new File(filename);
		
		if(! file.canRead()) {
			throw new IOException("Unable to open file: " + filename);
		}
		
		openDatabase();
		logger.info("Indexing file: {}", filename);
		
		indexedLines = 1;
		registerLine(indexedLines, 0);
		indexedLines++;
		
		try (
				final CountingInputStream inputStream 
				= new CountingInputStream(new BufferedInputStream(new FileInputStream(file)))
			) {
			
			while(inputStream.available() > 0) {
				final char readChar = (char) inputStream.read();
				if(readChar == '\n') {
					registerLine(indexedLines, inputStream.getBytesRead());
					indexedLines++;
				}
			}
		}
		
		logger.info("Indexing file done: {}", filename);
	}
	
	/**
	 * Open the Berkeley DB
	 * @throws IOException 
	 */
	protected void openDatabase() throws IOException {
		final EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(false);
		envConfig.setAllowCreate(true);
	    envConfig.setSharedCache(true);
	    	    
	    tmpDatabaseDir = Files.createTempDirectory(null);
		dbEnv = new Environment(tmpDatabaseDir.toFile(), envConfig);
		
		logger.info("Database dir is {}", tmpDatabaseDir);
		
		// Delete database on exit
		FileUtil.deleteDirOnExit(tmpDatabaseDir);

		final DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(true);
		dbConfig.setDeferredWrite(true);
		
		database = dbEnv.openDatabase(null, "lines", dbConfig);
	}
	
	/**
	 * Close the database
	 */
	public void close() {
		
		if(database != null) {
			database.close();
			database = null;
		}
		
		if(dbEnv != null) {
			dbEnv.close();
			dbEnv = null;
		}
	}

	/**
	 * Register a new line in the index file
	 * @param line
	 * @param pos
	 */
	protected void registerLine(final long line, final long bytePos) {
		final DatabaseEntry key = buildDatabaseEntry(line);
		final DatabaseEntry value = buildDatabaseEntry(bytePos);
		
		final OperationStatus status = database.put(null, key, value);
		
        if (status != OperationStatus.SUCCESS) {
            throw new RuntimeException("Data insertion, got status " + status);
        }
	}
	
	/**
	 * Get the db entry for a given long
	 * @param node
	 * @return
	 */
	protected static DatabaseEntry buildDatabaseEntry(final long id) {
		final ByteBuffer keyByteBuffer = DataEncoderHelper.longToByteBuffer(id);
		return new DatabaseEntry(keyByteBuffer.array());
	}
	
	/**
	 * Get the content of the line
	 * @param lineNumber
	 * @return
	 */
	public long locateLine(final long line) {
		
		if(database == null) {
			throw new IllegalArgumentException("No database is open, please index file first");
		}

		if(line >= indexedLines) {
			throw new IllegalArgumentException("Line " + line + " is higher then indexedLines: " + (indexedLines - 1));
		}
		
		final DatabaseEntry key = buildDatabaseEntry(line);
	    final DatabaseEntry value = new DatabaseEntry();
	    
	    final OperationStatus result = database.get(null, key, value, LockMode.DEFAULT);
	    
	    if (result != OperationStatus.SUCCESS) {
	        throw new RuntimeException("Data fetch got status " + result + " for " + line);
	    }
	    
	    final long bytePos = DataEncoderHelper.readLongFromByte(value.getData());
	    
	    return bytePos;
	}
	
	/**
	 * Get the amount of indexed lines
	 * @return
	 */
	public long getIndexedLines() {
		// Points to the next line
		return Math.max(0, indexedLines - 1);
	}

}
