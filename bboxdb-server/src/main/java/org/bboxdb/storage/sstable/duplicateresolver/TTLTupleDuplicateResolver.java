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
package org.bboxdb.storage.sstable.duplicateresolver;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.entity.Tuple;

public class TTLTupleDuplicateResolver implements DuplicateResolver<Tuple> {

	/**
	 * The TTL
	 */
	protected final long ttlInMicroseconds;
	
	/**
	 * The base time
	 */
	protected final long baseTime;

	public TTLTupleDuplicateResolver(final long ttl, final TimeUnit timeUnit) {
		// Tuple timestamp is in microseconds
		this(ttl, timeUnit, System.currentTimeMillis() * 1000);
	}
	
	public TTLTupleDuplicateResolver(final long ttl, final TimeUnit timeUnit, final long baseTime) {
		this.ttlInMicroseconds = timeUnit.toMicros(ttl);
		this.baseTime = baseTime;
	}

	@Override
	public void removeDuplicates(final List<Tuple> unconsumedDuplicates) {
		final long removalTimestamp = baseTime - ttlInMicroseconds;		
		unconsumedDuplicates.removeIf(t -> t.getVersionTimestamp() < removalTimestamp);
	}

}
