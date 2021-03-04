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
package org.bboxdb.network.client.future.client.helper;

import java.util.ArrayList;

import org.bboxdb.network.client.future.client.AbstractListFuture;
import org.bboxdb.storage.entity.MultiTuple;

public class ThreadedJoinedTupleListFutureIterator extends AbstractTheadedListFutureIterator<MultiTuple> {
	
	public ThreadedJoinedTupleListFutureIterator(final AbstractListFuture<MultiTuple> abstractListFuture) {
		super(abstractListFuture);
	}

	@Override
	protected MultiTuple buildQueueTerminal() {
		return new MultiTuple(new ArrayList<>(), new ArrayList<>());
	}

}
