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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	/**
	 * The hold locks
	 */
	private final Map<LockEntry, Object> locks;
	
	public LockManager() {
		this.locks = new ConcurrentHashMap<>();
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
	public boolean lockTuple(final Object lockObject, final String table, final String key, 
			final long version, final boolean deleteOnTimeout) {
		
		final LockEntry lockEntry = new LockEntry(table, key, version, deleteOnTimeout);
		
		synchronized (this) {
			if(locks.containsKey(lockEntry)) {
				return false;
			}
			
			locks.put(lockEntry, lockObject);
		}
		
		return true;
	}
	
	/**
	 * Remove all locks for the given connection
	 * @param lockObject
	 */
	public void removeAllLocksForObject(final Object lockObject) {
		locks.entrySet().removeIf(e -> e.getValue().equals(lockObject));
	}
	
	/**
	 * Remove locks for the given values 
	 * @param lockObject
	 * @param key
	 * @return 
	 */
	public boolean removeLockForConnectionAndKey(final Object lockObject, 
			final String table, final String key) {
		
		final Set<Entry<LockEntry, Object>> entrySet = locks.entrySet();
		
		return entrySet.removeIf(
				e -> e.getValue().equals(lockObject) 
				&& e.getKey().tableAndKeyMatches(table, key));
	}

	class LockEntry {

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
		
		public LockEntry(final String table, final String key, final long version, 
				final boolean deleteOnTimeout) {
			
			this.table = table;
			this.key = key;
			this.deleteOnTimeout = deleteOnTimeout;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			result = prime * result + ((table == null) ? 0 : table.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			LockEntry other = (LockEntry) obj;
			if (key == null) {
				if (other.key != null)
					return false;
			} else if (!key.equals(other.key))
				return false;
			if (table == null) {
				if (other.table != null)
					return false;
			} else if (!table.equals(other.table))
				return false;
			return true;
		}

		/**
		 * Get the key
		 * @return
		 */
		public String getKey() {
			return key;
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
	
	}
}
