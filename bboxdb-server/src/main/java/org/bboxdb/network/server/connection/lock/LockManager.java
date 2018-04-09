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
package org.bboxdb.network.server.connection.lock;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import io.prometheus.client.Gauge;

public class LockManager {
	
	/**
	 * The active locks counter
	 */
	private final static Gauge activeLocksTotal = Gauge.build()
			.name("bboxdb_network_tuple_locks_total")
			.help("Total amount of active tuple locks").register();
	
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
			activeLocksTotal.set(locks.size());
		}
		
		return true;
	}
	
	/**
	 * Remove all locks for the given connection
	 * @param lockObject
	 * @return 
	 */
	public List<LockEntry> removeAllLocksForObject(final Object lockObject) {
		final Predicate<? super LockEntry> removePredicate = e -> e.getLockObject().equals(lockObject);
		
		return removeForPredicate(removePredicate);
	}
	
	/**
	 * Get all locks an lock object
	 * @param lockObject
	 * @return
	 */
	public List<LockEntry> getAllLocksForObject(final Object lockObject) {
		final Predicate<? super LockEntry> predicate = e -> e.getLockObject().equals(lockObject);
		
		return locks.stream()
				.filter(e -> predicate.test(e))
				.collect(Collectors.toList());
	}
	
	/**
	 * Remove all elements for lock and sequence
	 * @param lockObject
	 * @param sequence
	 * @return
	 */
	public List<LockEntry> removeAllForLocksForObjectAndSequence(final Object lockObject, final short sequence) {
		final Predicate<? super LockEntry> removePredicate = e -> e.lockObjectAndSequenceMatches(lockObject, 
				sequence);
		
		return removeForPredicate(removePredicate);
	}

	/**
	 * Remove and return all elements for the given prediate from the
	 * locks data structure
	 * @param removePredicate
	 * @return
	 */
	private List<LockEntry> removeForPredicate(final Predicate<? super LockEntry> removePredicate) {
		
		final List<LockEntry> elementsToRemove = locks.stream()
				.filter(removePredicate)
				.collect(Collectors.toList());
				
		locks.removeAll(elementsToRemove);
		activeLocksTotal.set(locks.size());

		return elementsToRemove;
	}
	
	/**
	 * Remove locks for the given values 
	 * @param lockObject
	 * @param key
	 * @return 
	 */
	public List<LockEntry> removeLockForConnectionAndKey(final Object lockObject, 
			final String table, final String key) {
		
		final List<LockEntry> locksToRemove = locks.stream()
				.filter(e -> e.getLockObject().equals(lockObject) && e.tableAndKeyMatches(table, key))
				.collect(Collectors.toList());
		
		locks.removeAll(locksToRemove);
		activeLocksTotal.set(locks.size());
		
		return locksToRemove;
	}
}
