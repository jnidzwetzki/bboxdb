/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.network.client.future;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.network.client.ScalephantClient;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.queryprocessor.CloseableIterator;

public class TupleListFuture extends OperationFutureImpl<List<Tuple>> implements Iterable<Tuple> {
	
	private static final class TupleListFutureIterator implements CloseableIterator<Tuple> {
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
		 * The executor pool
		 */
		protected final ExecutorService executor = Executors.newCachedThreadPool();

		
		/**
		 * The Logger
		 */
		private final static Logger logger = LoggerFactory.getLogger(TupleListFutureIterator.class);

		
		public TupleListFutureIterator(final TupleListFuture tupleListFuture) {
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
						for(final Tuple tuple : tupleList) {
							tupleQueue.put(tuple);
						}
												
						// TODO: Handle paging
						
					} catch (InterruptedException | ExecutionException e) {
						logger.warn("Got exception while writing data to queue", e);
					} finally {
						addTerminalNE();
						logger.trace("Producer {} is done", resultId);
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
					// Ignore exception
					return false;
				}
										
				if(nextTuple == QUEUE_TERMINAL) {
					seenTerminals++;
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
	}

	/**
	 * Is the result complete or only a page?
	 */
	protected final Map<Integer, Boolean> resultComplete = new HashMap<>();
	
	/**
	 * The connections for the paging
	 */
	protected final Map<Integer, ScalephantClient> connections = new HashMap<>();
	
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
		
		if(! resultComplete.containsKey(resultComplete)) {
			return false;
		}

		return resultComplete.get(resultComplete);
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
	 * Set the scalephant connection for pagig
	 * @param resultId
	 * @param scalephantClient
	 */
	public void setConnectionForResult(final int resultId, final ScalephantClient scalephantClient) {
		checkFutureSize(resultId);

		connections.put(resultId, scalephantClient);
	}
	
	/**
	 * Get the ScalephantClient for the given resultId (needed to request next pages)
	 * @param resultId
	 * @return
	 */
	public ScalephantClient getConnectionForResult(final int resultId) {
		checkFutureSize(resultId);

		if(! connections.containsKey(resultId)) {
			logger.error("getConnectionForResult() called with id {}, but connection is unknown", resultId);
			return null;
		}
		
		return connections.get(resultId);
	}
	
	/**
	 * Return a iterator for all tupes
	 * @return
	 */
	@Override
	public CloseableIterator<Tuple> iterator() {
		if(! isDone() ) {
			throw new IllegalStateException("Future is not done, unable to build iterator");
		}
		
		if( isFailed() ) {
			throw new IllegalStateException("The future has failed, unable to build iterator");
		}
		
		return new TupleListFutureIterator(this);
	}
}
