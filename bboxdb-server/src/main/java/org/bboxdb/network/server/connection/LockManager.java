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
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
	
	/**
	 * The hold locks
	 */
	private final Map<LockEntry, ClientConnectionHandler> locks;
	
	public LockManager() {
		this.locks = new ConcurrentHashMap<>();
	}
	
	/**
	 * Lock a tuple
	 * @param connection
	 * @param table
	 * @param key
	 * @param version
	 * @return
	 */
	public boolean lockTuple(final ClientConnectionHandler connection, final String table, final String key, 
			final long version) {
		
		final LockEntry lockEntry = new LockEntry(table, key, version);
		
		synchronized (this) {
			if(locks.containsKey(lockEntry)) {
				return false;
			}
			
			locks.put(lockEntry, connection);
		}
		
		return true;
	}
	
	/**
	 * Remove all locks for the given connection
	 * @param connection
	 */
	public void removeAllLocksForConnection(final ClientConnectionHandler connection) {
		locks.entrySet().removeIf(e -> e.getValue().equals(connection));
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
		 * The version
		 */
		private long version;

		public LockEntry(final String table, final String key, final long version) {
			this.table = table;
			this.key = key;
			this.version = version;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((key == null) ? 0 : key.hashCode());
			result = prime * result + ((table == null) ? 0 : table.hashCode());
			result = prime * result + (int) (version ^ (version >>> 32));
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
			if (version != other.version)
				return false;
			return true;
		}
		
	}
}
