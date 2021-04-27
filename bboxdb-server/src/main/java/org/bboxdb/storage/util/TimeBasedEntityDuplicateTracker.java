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

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.bboxdb.storage.entity.EntityIdentifier;
import org.bboxdb.storage.entity.PagedTransferableEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeBasedEntityDuplicateTracker {

	/**
	 * The seen keys and versions
	 */
	protected Map<EntityIdentifier, Long> seenKeysAndVersions = new ConcurrentHashMap<>();

	/**
	 * The last eviction call
	 */
	protected long lastEviction = System.currentTimeMillis();
	
	/**
	 * The eviction time
	 */
	protected final long EVICT_TIME = TimeUnit.MINUTES.toMillis(10);
	
	/**
	 * The eviction wakeup time
	 */
	protected final long EVICT_WAKUP_TIME = TimeUnit.MINUTES.toMillis(1);

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
		
		if(System.currentTimeMillis() > lastEviction + EVICT_WAKUP_TIME) {
			logger.debug("Call eviction on the TimeBasedEntityDuplicateTracker");
			cleanUp();
		}
		
		if(! seenKeysAndVersions.containsKey(entity.getEntityIdentifier())) {
			seenKeysAndVersions.put(entity.getEntityIdentifier(), System.currentTimeMillis());
			return false;
		}
		
		return true;
	}

	/**
	 * Cleanup the old elements
	 */
	protected void cleanUp() {
		
		final Iterator<Entry<EntityIdentifier, Long>> iter = seenKeysAndVersions.entrySet().iterator();
		long removedElements = 0;
		
		while(iter.hasNext()) {
			final Entry<EntityIdentifier, Long> entry = iter.next();
			
			if(System.currentTimeMillis() > entry.getValue() + EVICT_TIME) {
				iter.remove();
				removedElements++;
			}
		}
		
		logger.debug("Removed {} elements from map", removedElements);
		
		lastEviction = System.currentTimeMillis();
	}
}
