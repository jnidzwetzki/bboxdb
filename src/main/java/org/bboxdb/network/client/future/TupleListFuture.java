/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.bboxdb.network.client.BBoxDBClient;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TupleListFuture extends OperationFutureImpl<List<Tuple>> implements Iterable<Tuple> {

	private static final class ThreadedTupleListFutureIterator implements CloseableIterator<Tuple> {
		/**
		 * The size of the transfer queue
		 */
		protected final static int QUEUE_SIZE = 25;
		
		/**
		 * The transfer queue
		 */
		protected final BlockingQueue<Tuple> tupleQueue = new LinkedBlockingQueue<Tuple>(QUEUE_SIZE);
		
		/**
		 * The amount of seen terminals, the iterator is exaustet, 
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
		 * The number of results, that needs to be queired
		 */
		protected final int futuresToQuery;
		
		/**
		 * The instance to iterate 
		 * 
		 * @param tupleListFuture
		 */
		protected final TupleListFuture tupleListFuture;
		
		/**
		 * The set is used to eleminate duplicate keys
		 */
		protected final Set<String> seenKeys = new HashSet<String>();
		
		/**
		 * The executor pool
		 */
		protected final ExecutorService executor = Executors.newCachedThreadPool();

		
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
				
				// Remove duplicates
				if(seenKeys.contains(nextTuple.getKey())) {
					nextTuple = null;
				} else {
					seenKeys.add(nextTuple.getKey());
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

	/**
	 * Is the result complete or only a page?
	 */
	protected final Map<Integer, Boolean> resultComplete = new HashMap<>();
	
	/**
	 * The connections for the paging
	 */
	protected final Map<Integer, BBoxDBClient> connections = new HashMap<>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleListFuture.class);

	public TupleListFuture() {
		super();
	}

	public TupleListFuture(final int numberOfFutures) {
		super(numberOfFutures);
	}

	
	/**
	 * Check whether the result is only a page or complete
	 * 
	 * @param resultId
	 * @return
	 */
	public boolean isCompleteResult(final int resultId) {
		checkFutureSize(resultId);
		
		if(! resultComplete.containsKey(resultId)) {
			return false;
		}

		return resultComplete.get(resultId);
	}

	/**
	 * Set the completed flag for a result
	 * 
	 * @param resultId
	 * @param completeResult
	 */
	public void setCompleteResult(final int resultId, final boolean completeResult) {
		checkFutureSize(resultId);

		resultComplete.put(resultId, completeResult);
	}
	
	/**
	 * Set the BBoxDB connection for paging
	 * @param resultId
	 * @param bboxdbClient
	 */
	public void setConnectionForResult(final int resultId, final BBoxDBClient bboxdbClient) {
		checkFutureSize(resultId);

		connections.put(resultId, bboxdbClient);
	}
	
	/**
	 * Get the bboxdbClient for the given resultId (needed to request next pages)
	 * @param resultId
	 * @return
	 */
	public BBoxDBClient getConnectionForResult(final int resultId) {
		checkFutureSize(resultId);

		if(! connections.containsKey(resultId)) {
			logger.error("getConnectionForResult() called with id {}, but connection is unknown", resultId);
			return null;
		}
		
		return connections.get(resultId);
	}
		
	/**
	 * Return a iterator for all tuples
	 * @return
	 */
	@Override
	public Iterator<Tuple> iterator() {
		if(! isDone() ) {
			throw new IllegalStateException("Future is not done, unable to build iterator");
		}
		
		if( isFailed() ) {
			throw new IllegalStateException("The future has failed, unable to build iterator");
		}
		
		// Is at least result paged? So, we use the threaded iterator 
		// that requests more tuples/pages in the background
		final boolean pagedResult = resultComplete.values().stream().anyMatch(e -> e == false);
		
		if(pagedResult) {
			return new ThreadedTupleListFutureIterator(this);
		} else {
			return createSimpleIterator();
		}
		
	}

	/**
	 * Returns a simple iterator for non paged results
	 * @return
	 */
	protected Iterator<Tuple> createSimpleIterator() {
		final Map<String, Tuple> allTuples = new HashMap<String, Tuple>();
		
		for(int i = 0; i < getNumberOfResultObjets(); i++) {
			try {
				final List<Tuple> tupleResult = get(i);
				
				if(tupleResult != null) {
					for(final Tuple tuple : tupleResult) {
						// Hash map <Key, Tuple> is used to eleminate duplicates
						allTuples.put(tuple.getKey(), tuple);
					}
				}
				
			} catch (Exception e) {
				logger.error("Got exception while iterating", e);
			}
		}
		
		return allTuples.values().iterator();
	}
}
