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
package org.bboxdb.storage.util;

import java.util.HashSet;
import java.util.Set;

import org.bboxdb.storage.entity.EntityIdentifier;
import org.bboxdb.storage.entity.PagedTransferableEntity;

public class EntityDuplicateTracker {

	/**
	 * The seen keys and versions
	 */
	protected Set<EntityIdentifier> seenKeysAndVersions = new HashSet<>();
	
	public boolean isElementAlreadySeen(final PagedTransferableEntity entity) {
		
		if(! seenKeysAndVersions.contains(entity.getEntityIdentifier())) {
			seenKeysAndVersions.add(entity.getEntityIdentifier());
			return false;
		}
		
		return true;
	}
	
}
