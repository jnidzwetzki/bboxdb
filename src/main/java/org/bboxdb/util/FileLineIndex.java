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
package org.bboxdb.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

import org.apache.commons.compress.utils.CountingInputStream;
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
		
		long line = 1;
		registerLine(line, 0);
		line++;
		
		try (
				final CountingInputStream inputStream 
				= new CountingInputStream(new BufferedInputStream(new FileInputStream(file)))
			) {
			
			while(inputStream.available() > 0) {
				final char readChar = (char) inputStream.read();
				if(readChar == '\n') {
					registerLine(line, inputStream.getBytesRead());
					line++;
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
		deleteDatabaseDirOnExit();

		final DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(true);
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
	 * Delete the database dir on exit
	 */
	protected void deleteDatabaseDirOnExit() {
		Runtime.getRuntime().addShutdownHook(new Thread() {
	        @Override
	        public void run() {
	        	
	        	close();
	        	
	        	final Path directory = tmpDatabaseDir.toAbsolutePath();
	        	try {
					Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
					   @Override
					   public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					       Files.delete(file);
					       return FileVisitResult.CONTINUE;
					   }

					   @Override
					   public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
					       Files.delete(dir);
					       return FileVisitResult.CONTINUE;
					   }
					});
				} catch (IOException e) {
					System.err.println("Got Exception: " + e);
				}
	        }
	    });
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
	public static DatabaseEntry buildDatabaseEntry(final long id) {
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
		
		final DatabaseEntry key = buildDatabaseEntry(line);
	    final DatabaseEntry value = new DatabaseEntry();
	    
	    final OperationStatus result = database.get(null, key, value, LockMode.DEFAULT);
	    
	    if (result != OperationStatus.SUCCESS) {
	        throw new RuntimeException("Data fetch got status " + result);
	    }
	    
	    final long bytePos = DataEncoderHelper.readLongFromByte(value.getData());
	    
	    return bytePos;
	}

}
