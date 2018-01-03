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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.entity.Tuple;

public class TTLAndVersionTupleDuplicateResolver implements DuplicateResolver<Tuple> {
	
	protected final List<DuplicateResolver<Tuple>> duplicateResolver;

	public TTLAndVersionTupleDuplicateResolver(final long ttl, final TimeUnit timeUnit, final int versions) {
		this(ttl, timeUnit, versions, System.currentTimeMillis());
	}
	
	public TTLAndVersionTupleDuplicateResolver(final long ttl, final TimeUnit timeUnit,
			final int versions, final long baseTime) {
		
		duplicateResolver = new ArrayList<DuplicateResolver<Tuple>>();
		
		// The resolver sorts the key by version timestamp
		duplicateResolver.add(new TTLTupleDuplicateResolver(ttl, timeUnit, baseTime));
		
		// If more then versions tuples are available, the oldest tuples are removed
		duplicateResolver.add(new VersionTupleDuplicateResolver(versions));
	}

	@Override
	public void removeDuplicates(final List<Tuple> unconsumedDuplicates) {
		duplicateResolver.forEach(d -> d.removeDuplicates(unconsumedDuplicates));
	}

}
