/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.storage.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.bboxdb.storage.entity.EntityIdentifier;
import org.bboxdb.storage.entity.PagedTransferableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedEntityDuplicateTracker {

	/**
	 * The seen keys and versions
	 */
	protected Map<EntityIdentifier, Long> seenKeysAndVersions = new HashMap<>();
	
	/**
	 * The lock of the map
	 */
	protected final ReentrantLock mapLock = new ReentrantLock();

	/**
	 * The last eviction call
	 */
	protected long lastEviction = System.currentTimeMillis();
	
	/**
	 * The eviction time
	 */
	protected final long EVICT_TIME = TimeUnit.MINUTES.toMillis(2);
	
	/**
	 * The eviction wakeup time
	 */
	protected final long EVICT_WAKUP_TIME = TimeUnit.SECONDS.toMillis(30);

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TimeBasedEntityDuplicateTracker.class);
	
	/**
	 * Is the element already seen ? 
	 * @param entity
	 * @return true or false
	 */
	public boolean isElementAlreadySeen(final PagedTransferableEntity entity) {
		
		// Cleanup old elements if needed
		cleanUpIfNeeded();
		
		final EntityIdentifier entityIdentifier = entity.getEntityIdentifier();
		
		Long oldValue = null;
		
		mapLock.lock();
		try {
			oldValue = seenKeysAndVersions.put(entityIdentifier, System.currentTimeMillis());
		} finally {
			mapLock.unlock();
		}
		
		if(oldValue == null) {
			return false;
		}
		
		return true;
	}

	/**
	 * Cleanup the old elements
	 */
	protected void cleanUpIfNeeded() {
		long removedElements = 0;
		long mapSizeAfterClean = 0;

		mapLock.lock();
		try {
			if(System.currentTimeMillis() <= lastEviction + EVICT_WAKUP_TIME) {
				return;
			}
			
			logger.debug("Call eviction on the TimeBasedEntityDuplicateTracker");

			final Iterator<Entry<EntityIdentifier, Long>> iter = seenKeysAndVersions.entrySet().iterator();
			
			while(iter.hasNext()) {
				final Entry<EntityIdentifier, Long> entry = iter.next();
				
				if(System.currentTimeMillis() > entry.getValue() + EVICT_TIME) {
					iter.remove();
					removedElements++;
				}
			}
			
			mapSizeAfterClean = seenKeysAndVersions.size();
		} finally {
			lastEviction = System.currentTimeMillis();
			mapLock.unlock();
		}
		
		logger.debug("Removed {} elements from map, remaining entries {}", removedElements, mapSizeAfterClean);
	}
}
