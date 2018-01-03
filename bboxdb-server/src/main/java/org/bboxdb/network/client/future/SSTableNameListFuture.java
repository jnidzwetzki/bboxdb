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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bboxdb.storage.entity.TupleStoreName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableNameListFuture extends OperationFutureImpl<List<TupleStoreName>> implements Iterable<TupleStoreName> {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableNameListFuture.class);

	public SSTableNameListFuture() {
		super();
	}

	public SSTableNameListFuture(final int numberOfFutures) {
		super(numberOfFutures);
	}

	@Override
	public Iterator<TupleStoreName> iterator() {
		
		try {
			waitForAll();

			final List<TupleStoreName> resultList = new ArrayList<TupleStoreName>();
			
			for(final FutureImplementation<List<TupleStoreName>> future : futures) {
				resultList.addAll(future.get());
			}
			
			return resultList.iterator();
			
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Got an exception while creating iterator", e);
			return new ArrayList<TupleStoreName>().iterator();
		}
	}

}
