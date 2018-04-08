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

public class LockEntry {

	/**
	 * The lock object
	 */
	private final Object lockObject;

	/**
	 * The sequence number
	 */
	private final short sequenceNumber;
	
	/**
	 * The table
	 */
	private final String table;
	
	/**
	 * The key
	 */
	private final String key;

	/**
	 * The version to lock
	 */
	private final long version;
	
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
		this.version = version;
		this.deleteOnTimeout = deleteOnTimeout;
	}
	
	/**
	 * Get the table
	 * @return
	 */
	public String getTable() {
		return table;
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
	 * Get the lock version
	 * @return
	 */
	public long getVersion() {
		return version;
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
	public boolean lockObjectAndSequenceMatches(final Object lockObject, final short sequence) {
		return this.lockObject.equals(lockObject)  && this.sequenceNumber == sequence;
	}

	@Override
	public String toString() {
		return "LockEntry [lockObject=" + lockObject + ", sequenceNumber=" + sequenceNumber + ", table=" + table
				+ ", key=" + key + ", version=" + version + ", deleteOnTimeout=" + deleteOnTimeout + "]";
	}

}