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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.util.CloseableIterator;
import org.bboxdb.storage.util.EntityDuplicateRemover;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedTupleListFutureIterator implements CloseableIterator<Tuple>{
	
	/**
	 * The size of the transfer queue
	 */
	protected final static int QUEUE_SIZE = 25;
	
	/**
	 * The transfer queue
	 */
	protected final BlockingQueue<Tuple> tupleQueue = new LinkedBlockingQueue<Tuple>(QUEUE_SIZE);
	
	/**
	 * The amount of seen terminals, the iterator is exhausted, 
	 * when all producers are terminated and the queue is empty
	 */
	protected int seenTerminals = 0;
	
	/**
	 * The terminal (or poison) element
	 */
	protected final static Tuple QUEUE_TERMINAL = new Tuple("", BoundingBox.EMPTY_BOX, "".getBytes());
	
	/**
	 * The next tuple for the next operation
	 */
	protected Tuple nextTuple = null;
	
	/**
	 * The number of results, that needs to be queried
	 */
	protected final int futuresToQuery;
	
	/**
	 * The instance to iterate 
	 * 
	 * @param tupleListFuture
	 */
	protected final TupleListFuture tupleListFuture;

	/**
	 * The executor pool
	 */
	protected final ExecutorService executor = Executors.newCachedThreadPool();

	/**
	 * The tuple duplicate remover
	 */
	protected final EntityDuplicateRemover tupleDuplicateRemover = new EntityDuplicateRemover();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ThreadedTupleListFutureIterator.class);

	
	public ThreadedTupleListFutureIterator(final TupleListFuture tupleListFuture) {
		this.tupleListFuture = tupleListFuture;
		this.futuresToQuery = tupleListFuture.getNumberOfResultObjets();
		
		for(int i = 0; i < tupleListFuture.getNumberOfResultObjets(); i++) {
			setupProducer(i);
		}
	}
	
	/**
	 * Setup the worker that fetches the data from the futures
	 */
	public void setupProducer(final int resultId) {
		
		logger.trace("Start producer for {}", resultId);
		
		final Runnable producer = new Runnable() {
			
			@Override
			public void run() {

				try {
					final List<Tuple> tupleList = tupleListFuture.get(resultId);
					
					addTupleListToQueue(tupleList);
								
					if(! tupleListFuture.isCompleteResult(resultId)) {
						handleAdditionalPages();
					}
					
				} catch (ExecutionException e) {
					logger.warn("Got exception while writing data to queue", e);
				} catch (InterruptedException e) {
					logger.warn("Got exception while writing data to queue", e);
					Thread.currentThread().interrupt();
				} finally {
					addTerminalNE();
					logger.trace("Producer {} is done", resultId);
				}
			}
			
			/**
			 * Request and add the additional pages to the queue
			 * @throws ExecutionException 
			 * @throws InterruptedException 
			 */
			protected void handleAdditionalPages() throws InterruptedException, ExecutionException {
				final BBoxDBClient bboxdbClient = tupleListFuture.getConnectionForResult(resultId);
				final short queryRequestId = tupleListFuture.getRequestId(resultId);
				
				if(bboxdbClient == null) {
					logger.error("Unable to get connection for paging: {}", resultId);
					return;
				}
				
				TupleListFuture nextPage = null;
				do {
					 nextPage = (TupleListFuture) bboxdbClient.getNextPage(queryRequestId);

					 nextPage.waitForAll();

					 if(nextPage.isFailed()) {
						 logger.error("Requesting next page failed! Query result is incomplete: {}", nextPage.getAllMessages());
						 return;
					 }

					 // Query is send to one server, so the number of
					 // result objects should be 1
					 if(nextPage.getNumberOfResultObjets() != 1) {
						 logger.error("Got a non expected number of result objects {}", nextPage.getNumberOfResultObjets());
					 }

					 addTupleListToQueue(nextPage.get(0));
					 
				} while(! nextPage.isCompleteResult(0));
			}

			/**
			 * Add the tuple list into the queue
			 * @param tupleList
			 * @throws InterruptedException
			 */
			protected void addTupleListToQueue(final List<Tuple> tupleList) throws InterruptedException {
				for(final Tuple tuple : tupleList) {
					tupleQueue.put(tuple);
				}
			}

			/**
			 * Add the terminal to the queue
			 */
			protected void addTerminalNE() {
				try {
					tupleQueue.put(QUEUE_TERMINAL);
				} catch (InterruptedException e) {
					// Got the interrupted exception while addint the 
					// terminal, ignoring
				}
			}
		};
		
		executor.submit(producer);
	}

	@Override
	public boolean hasNext() {
		
		if(nextTuple != null) {
			logger.warn("Last tuple was not requested, did you call next before?");
			nextTuple = null;
		}
		
		while(nextTuple == null) {
			
			// All worker are done
			if(seenTerminals == futuresToQuery) {
				return (nextTuple != null);
			}
			
			try {
				// Wait until element is available
				nextTuple = tupleQueue.take(); 
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
									
			if(nextTuple == QUEUE_TERMINAL) {
				seenTerminals++;
				nextTuple = null;
				continue;
			}
			
			// Tuple was received from another instance
			if(tupleDuplicateRemover.isElementAlreadySeen(nextTuple)) {
				nextTuple = null;
			}
		}
		
		return true;
	}

	@Override
	public Tuple next() {
		if(nextTuple == null) {
			throw new IllegalStateException("Tuple is null, did you called hasNext before?");
		}
		
		final Tuple resultTuple = nextTuple;
		nextTuple = null;
		return resultTuple;
	}

	@Override
	public void close() throws Exception {
		logger.trace("Close called on interator");
		executor.shutdown();
	}
	
	@Override
	protected void finalize() throws Throwable {
		close();
		super.finalize();
	}
}
