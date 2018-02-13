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

import org.bboxdb.storage.sstable.duplicateresolver.DoNothingDuplicateResolver;

public class FutureHelper {

	/**
	 * Create and return an empty result future
	 * @return
	 */
	public static EmptyResultFuture getFailedEmptyResultFuture(final String failedMessage) {
		final EmptyResultFuture future = new EmptyResultFuture(1);
		future.setMessage(0, failedMessage);
		future.setFailedState();
		future.fireCompleteEvent();
		return future;
	}
	
	/**
	 * Create and return an empty tuple list future
	 * @return
	 */
	public static TupleListFuture getFailedTupleListFuture(final String failedMessage, final String table) {
		final TupleListFuture future = new TupleListFuture(1, new DoNothingDuplicateResolver(), table);
		future.setMessage(0, failedMessage);
		future.setFailedState();
		future.fireCompleteEvent();
		return future;
	}
	
	/**
	 * Create and return an empty joinedtuple list future
	 * @return
	 */
	public static JoinedTupleListFuture getFailedJoinedTupleListFuture(final String failedMessage) {
		final JoinedTupleListFuture future = new JoinedTupleListFuture(1);
		future.setMessage(0, failedMessage);
		future.setFailedState();
		future.fireCompleteEvent();
		return future;
	}
	
	/**
	 * Create a failed SSTableNameListFuture
	 * @return
	 */
	public static SSTableNameListFuture getFailedSStableNameFuture(final String errorMessage) {
		final SSTableNameListFuture clientOperationFuture = new SSTableNameListFuture(1);
		clientOperationFuture.setMessage(0, errorMessage);
		clientOperationFuture.setFailedState();
		clientOperationFuture.fireCompleteEvent(); 
		return clientOperationFuture;
	}

}
