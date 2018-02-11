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
package org.bboxdb.network.client.future;

import java.util.Iterator;
import java.util.List;

import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.util.EntityDuplicateTracker;

public class JoinedTupleListFuture extends AbstractListFuture<JoinedTuple>{

	public JoinedTupleListFuture(final int numberOfFutures) {
		super(numberOfFutures);
	}
	
	public JoinedTupleListFuture() {
		super();
	}

	@Override
	protected Iterator<JoinedTuple> createThreadedIterator() {
		return new ThreadedJoinedTupleListFutureIterator(this);
	}

	@Override
	protected Iterator<JoinedTuple> createSimpleIterator() {
		final List<JoinedTuple> allTuples = getListWithAllResults();
		final EntityDuplicateTracker entityDuplicateTracker = new EntityDuplicateTracker();
		
		final Iterator<JoinedTuple> iterator = allTuples.iterator();
		while(iterator.hasNext()) {
			final JoinedTuple nextElement = iterator.next();
			
			if(entityDuplicateTracker.isElementAlreadySeen(nextElement)) {
				iterator.remove();
			}
		}
		
		return allTuples.iterator();
	}

}
