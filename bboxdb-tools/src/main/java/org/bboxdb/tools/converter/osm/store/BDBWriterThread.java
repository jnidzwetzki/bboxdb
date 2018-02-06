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
package org.bboxdb.tools.converter.osm.store;

import java.util.List;

import org.bboxdb.commons.concurrent.ExceptionSafeThread;
import org.bboxdb.tools.converter.osm.util.SerializableNode;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

public class BDBWriterThread extends ExceptionSafeThread {

	protected final List<SerializableNode> pendingWriteQueue;
	protected final Environment environment;
	protected final Database database;
	
	public BDBWriterThread(final List<SerializableNode> pendingWriteQueue, 
			final Environment environment, final Database database) {
		
		this.pendingWriteQueue = pendingWriteQueue;
		this.environment = environment;
		this.database = database;
	}

	@Override
	protected void runThread() {
		while(! Thread.currentThread().isInterrupted()) {
			
			SerializableNode nodeToProcess = null;
			
			synchronized (pendingWriteQueue) {
				
				while(pendingWriteQueue.isEmpty()) {
					try {
						pendingWriteQueue.wait();
					} catch (InterruptedException e) {
						// Handle interrupt directly
						return;
					}
				}
				
				nodeToProcess = pendingWriteQueue.get(0);
			}
			
			storeNode(nodeToProcess);
			
			// Remove element after it is stored in BDB
			synchronized (pendingWriteQueue) {
				pendingWriteQueue.remove(0);
				pendingWriteQueue.notifyAll();
			}
		}
	}
	
	@SuppressWarnings("unused")
	protected void storeNode(final SerializableNode node) {
		final byte[] nodeBytes = node.toByteArray();

		Transaction txn = null;
		if(OSMBDBNodeStore.USE_TRANSACTIONS) {
			txn = environment.beginTransaction(null, null);
		}
		
		final DatabaseEntry key = OSMBDBNodeStore.buildDatabaseKeyEntry(node.getId());
		final DatabaseEntry value = new DatabaseEntry(nodeBytes);
		final OperationStatus status = database.put(txn, key, value);
	
        if (status != OperationStatus.SUCCESS) {
            throw new RuntimeException("Data insertion got status " + status);
        }
        
        if(txn != null) {
        	txn.commit();
        }
	}
}
