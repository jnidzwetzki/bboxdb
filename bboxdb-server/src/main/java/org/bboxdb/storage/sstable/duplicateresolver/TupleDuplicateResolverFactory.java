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

import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;

public class TupleDuplicateResolverFactory {

	/**
	 * Get the duplicate resolver for the tuple store configuration
	 * @return
	 */
	public static DuplicateResolver<Tuple> build(final TupleStoreConfiguration tupleStoreConfiguration) {		
		final boolean allowDuplicates = tupleStoreConfiguration.isAllowDuplicates();
		final int versions = tupleStoreConfiguration.getVersions();
		final long ttl = tupleStoreConfiguration.getTTL();
		
		// Remove all tuples, except the newest
		if(! allowDuplicates) {
			return new NewestTupleDuplicateResolver();
		} 

		// Remove tuples by time and version
		if(versions > 0 && ttl > 0) {
			return new TTLAndVersionTupleDuplicateResolver(ttl, TimeUnit.MILLISECONDS, versions);
		}
		
		// Remove tuples by versions
		if(versions > 0) {
			return new VersionTupleDuplicateResolver(versions);
		}
		
		// Remove tuples by time
		if(ttl > 0) {
			return new TTLTupleDuplicateResolver(ttl, TimeUnit.MILLISECONDS);
		}
		
		// Don't remove old tuples
		return new DoNothingDuplicateResolver();
	}
	
}
