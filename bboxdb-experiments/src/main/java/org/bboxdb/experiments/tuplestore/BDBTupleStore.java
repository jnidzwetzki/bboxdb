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
package org.bboxdb.experiments.tuplestore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.TupleHelper;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BDBTupleStore implements TupleStore {

	/**
	 * Use transactions
	 */
	public static boolean USE_TRANSACTIONS = false;
	
	/**
	 * The DB environment
	 */
	private Environment environment;

	/**
	 * The database
	 */
	private Database database;

	/**
	 * The database dir
	 */
	private File dir;
	
	public BDBTupleStore(final File dir) {
		this.dir = dir;
	}
	
	@Override
	public void writeTuple(final Tuple tuple) throws IOException {
		Transaction txn = null;
		
		if(USE_TRANSACTIONS) {
			txn = environment.beginTransaction(null, null);
		}
		
		final byte[] tupleBytes = TupleHelper.tupleToBytes(tuple);
		final DatabaseEntry key = new DatabaseEntry(tuple.getKey().getBytes());
		final DatabaseEntry value = new DatabaseEntry(tupleBytes);
		final OperationStatus status = database.put(txn, key, value);
	
        if (status != OperationStatus.SUCCESS) {
            throw new RuntimeException("Data insertion got status " + status);
        }
        
        if(txn != null) {
        	txn.commit();
        }
	}

	@Override
	public Tuple readTuple(final String key) throws IOException {
		final DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
	    final DatabaseEntry value = new DatabaseEntry();
	    
		Transaction txn = null;

		if(USE_TRANSACTIONS) {
			txn = environment.beginTransaction(null, null);
		}
		
	    final OperationStatus result = database.get(null, keyEntry, value, LockMode.DEFAULT);
	    
	    if (result != OperationStatus.SUCCESS) {
	        throw new RuntimeException("Data fetch got status " + result + " for " + key);
	    }
	    
        if(txn != null) {
        	txn.commit();
        }
        
        final ByteBuffer byteBuffer = ByteBuffer.wrap(value.getData());
        
        return TupleHelper.decodeTuple(byteBuffer);
	}

	@Override
	public void close() throws Exception {
		if(database != null) {
			database.close();
			database = null;
		}
		
		if(environment != null) {
			environment.close();
			environment = null;
		}
	}

	@Override
	public void open() throws Exception {
		final EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setTransactional(USE_TRANSACTIONS);
		envConfig.setAllowCreate(true);

		environment = new Environment(dir, envConfig);

		Transaction txn = null;
		if(USE_TRANSACTIONS) {
			txn = environment.beginTransaction(null, null);
		}
		
		final DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(USE_TRANSACTIONS);
		dbConfig.setAllowCreate(true);
		//dbConfig.setSortedDuplicates(true);
		dbConfig.setDeferredWrite(true);
		//dbConfig.setKeyPrefixing(true);
		//dbConfig.setNodeMaxEntries(128);
	
		database = environment.openDatabase(txn, "test", dbConfig);

		if(txn != null) {
			txn.commit();
		}	
	}
}