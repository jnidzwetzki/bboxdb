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
package org.bboxdb.network.server.connection;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

public class LockManager {
	
	/**
	 * The hold locks
	 */
	private final Set<LockEntry> locks;
	
	public LockManager() {
		this.locks = Sets.newConcurrentHashSet(); 
	}
	
	/**
	 * Lock a tuple
	 * @param lockObject
	 * @param table
	 * @param key
	 * @param version
	 * @param deleteOnTimeout 
	 * @return
	 */
	public boolean lockTuple(final Object lockObject, final short sequenceNumber, final String table,
			final String key, final long version, final boolean deleteOnTimeout) {
		
		final LockEntry lockEntry = new LockEntry(lockObject, sequenceNumber, table, key, 
				version, deleteOnTimeout);
		
		synchronized (this) {
			if(locks.stream().anyMatch(e -> e.tableAndKeyMatches(table, key))) {
				return false;
			}
			
			locks.add(lockEntry);
		}
		
		return true;
	}
	
	/**
	 * Remove all locks for the given connection
	 * @param lockObject
	 * @return 
	 */
	public Set<LockEntry> removeAllLocksForObject(final Object lockObject) {
		final Predicate<? super LockEntry> removePredicate = e -> e.getLockObject().equals(lockObject);
		return removeForPredicate(removePredicate);
	}

	/**
	 * Remove and return all elements for the given prediate from the
	 * locks data structure
	 * @param removePredicate
	 * @return
	 */
	private Set<LockEntry> removeForPredicate(final Predicate<? super LockEntry> removePredicate) {
		
		final Set<LockEntry> elementsToRemove = locks.stream()
				.filter(removePredicate)
				.collect(Collectors.toSet());
		
		locks.removeAll(elementsToRemove);
		
		return elementsToRemove;
	}
	
	/**
	 * Remove locks for the given values 
	 * @param lockObject
	 * @param key
	 * @return 
	 */
	public boolean removeLockForConnectionAndKey(final Object lockObject, 
			final String table, final String key) {
				
		return locks.removeIf(
				e -> e.getLockObject().equals(lockObject) 
				&& e.tableAndKeyMatches(table, key));
	}

	class LockEntry {

		/**
		 * The lock object
		 */
		private Object lockObject;

		/**
		 * The sequence number
		 */
		private short sequenceNumber;
		
		/**
		 * The table
		 */
		private String table;
		
		/**
		 * The key
		 */
		private String key;

		/**
		 * Delete on timeout flag
		 */
		private boolean deleteOnTimeout;
		
		public LockEntry(final Object lockObject, final short sequenceNumber, final String table, 
				final String key, final long version, final boolean deleteOnTimeout) {
			
			this.lockObject = lockObject;
			this.sequenceNumber = sequenceNumber;
			this.table = table;
			this.key = key;
			this.deleteOnTimeout = deleteOnTimeout;
		}
		
		/**
		 * Get the key
		 * @return
		 */
		public String getKey() {
			return key;
		}
		
		/**
		 * Get the lock object
		 * @return
		 */
		public Object getLockObject() {
			return lockObject;
		}
		
		/**
		 * Get the sequence number
		 * @return
		 */
		public short getSequenceNumber() {
			return sequenceNumber;
		}
		
		/**
		 * Get the delete on timeout flag
		 * @return
		 */
		public boolean isDeleteOnTimeout() {
			return deleteOnTimeout;
		}
		
		/**
		 * Compare on table and key
		 * @param table
		 * @param key
		 * @return
		 */
		public boolean tableAndKeyMatches(final String table, final String key) {
			return this.table.equals(table) && this.key.equals(key);
		}
		
		/**
		 * Compare lock object and sequence
		 * @param lockObject
		 * @param sequence
		 * @return
		 */
		public boolean lockObjectAndSequenceMathces(final Object lockObject, final short sequence) {
			return this.lockObject.equals(lockObject)  && this.sequenceNumber == sequence;
		}
	}
}
