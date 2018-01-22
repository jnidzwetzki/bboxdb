package org.bboxdb.storage.queryprocessor;

import java.util.List;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.queryprocessor.queryplan.BoundingBoxQueryPlan;
import org.bboxdb.storage.queryprocessor.queryplan.QueryPlan;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.util.CloseableIterator;

public class JoinQueryProcessor {
	
	/**
	 * The first query processor
	 */
	private final CloseableIterator<JoinedTuple> tupleStreamSource;
	
	/**
	 * The second query processor
	 */
	private final TupleStoreManager tupleStoreManager;

	public JoinQueryProcessor(final CloseableIterator<JoinedTuple> tupleStreamSource, 
			final TupleStoreManager tupleStoreManager) {

		this.tupleStreamSource = tupleStreamSource;
		this.tupleStoreManager = tupleStoreManager;
	}
	
	public JoinQueryProcessor(final TupleStoreManager tupleStoreManager1, 
		final TupleStoreManager tupleStoreManager2, final BoundingBox boundingBox) {
		
		this.tupleStreamSource = new QueryProcessorAdapter(tupleStoreManager1, boundingBox);
		this.tupleStoreManager = tupleStoreManager2;
	}

	/**
	 * Close all iterators
	 */
	private void closeIterators() {
		CloseableHelper.closeWithoutException(tupleStreamSource);
	}
	
	/**
	 * Get the query processing result
	 * @return
	 */
	public CloseableIterator<JoinedTuple> iterator() {
		
		return new CloseableIterator<JoinedTuple>() {
			
			private JoinedTuple tupleToJoin = null;
			
			private CloseableIterator<Tuple> candidatesForCurrentTuple = null;
			
			private JoinedTuple nextTuple = null;

			@Override
			public boolean hasNext() {
				
				// Join partner exhaused, try next tuple
				if(candidatesForCurrentTuple != null) {
					if(candidatesForCurrentTuple.hasNext()) {
						return true;
					} else {
						tupleToJoin = null;
						CloseableHelper.closeWithoutException(candidatesForCurrentTuple);
					}
				}
				
				while(tupleToJoin == null) {
					// Fetch next tuple from stream source
					if(! tupleStreamSource.hasNext()) { 
						return false;
					} else {
						tupleToJoin = tupleStreamSource.next();
						
						final QueryPlan queryplan = new BoundingBoxQueryPlan(tupleToJoin.getBoundingBox());
						final SelectionQueryProcessor queryProcessor = 
								new SelectionQueryProcessor(queryplan, tupleStoreManager);
						
						this.candidatesForCurrentTuple = queryProcessor.iterator();
					} 
					
					// No tuples to join in tuple store manager, try next tuple
					if(! candidatesForCurrentTuple.hasNext()) {
						tupleToJoin = null;
					}
				}
				
				final List<String> tupleStoreNames = tupleToJoin.getTupleStoreNames();
				final List<Tuple> tuples = tupleToJoin.getTuples();
				
				tupleStoreNames.add(tupleStoreManager.getTupleStoreName().getFullname());
				tuples.add(candidatesForCurrentTuple.next());
				nextTuple = new JoinedTuple(tuples, tupleStoreNames);
				
				return true;
			}

			@Override
			public JoinedTuple next() {
				
				if(nextTuple == null) {
					throw new IllegalArgumentException("Next tuple is null, do you forget to call hasNext()?");
				}
				
				final JoinedTuple returnTuple = nextTuple;
				nextTuple = null;
				return returnTuple;
			}

			@Override
			public void close() throws Exception {
				closeIterators();
			}
		};
		
	}

}

class QueryProcessorAdapter implements CloseableIterator<JoinedTuple> {
	
	/**
	 * The tuple iterator
	 */
	private CloseableIterator<Tuple> tupleIterator;
	
	/**
	 * The tuple store name
	 */
	private TupleStoreName tupleStoreName;

	public QueryProcessorAdapter(TupleStoreManager tupleStoreManager, BoundingBox boundingBox) {
		this.tupleStoreName = tupleStoreManager.getTupleStoreName();
				
		final QueryPlan queryplan = new BoundingBoxQueryPlan(boundingBox);
		final SelectionQueryProcessor queryProcessor = 
				new SelectionQueryProcessor(queryplan, tupleStoreManager);
		
		this.tupleIterator = queryProcessor.iterator();
	}

	@Override
	public boolean hasNext() {
		return tupleIterator.hasNext();
	}

	@Override
	public JoinedTuple next() {
		final Tuple nextTuple = tupleIterator.next();
		return new JoinedTuple(nextTuple, tupleStoreName.getFullname());
	}

	@Override
	public void close() throws Exception {
		tupleIterator.close();
	}
	
}