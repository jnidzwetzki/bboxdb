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

import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.storage.entity.Tuple;

public class VersionTupleDuplicateResolver implements DuplicateResolver<Tuple> {

	/**
	 * The versions
	 */
	protected final int versions;

	public VersionTupleDuplicateResolver(final int versions) {
		this.versions = versions;
	}

	@Override
	public void removeDuplicates(final List<Tuple> unconsumedDuplicates) {
		unconsumedDuplicates.sort((t1, t2) -> (Long.compare(t1.getVersionTimestamp(), t2.getVersionTimestamp())));
		
		final int elementsToRemove = Math.max(0, unconsumedDuplicates.size() - versions);
		int removed = 0;

		for(Iterator<Tuple> iter = unconsumedDuplicates.iterator(); iter.hasNext(); ) {
			iter.next();
			if(removed < elementsToRemove) {
				iter.remove();
				removed++;
			}
		}
	}

}
