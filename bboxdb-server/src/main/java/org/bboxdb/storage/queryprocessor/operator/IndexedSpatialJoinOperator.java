package org.bboxdb.storage.queryprocessor.operator;

import java.util.Iterator;
import java.util.List;

import org.bboxdb.commons.CloseableHelper;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;

public class IndexedSpatialJoinOperator implements Operator {
	
	/**
	 * The first query processor
	 */
	private final Iterator<JoinedTuple> tupleStreamSource;
	
	/**
	 * The tuple stream operator
	 */
	private final Operator tupleStreamOperator;
	
	/**
	 * The index reader
	 */
	private final SpatialIndexReadOperator indexReader;

	public IndexedSpatialJoinOperator(final Operator tupleStreamOperator, 
			final SpatialIndexReadOperator indexReader) {

		this.tupleStreamOperator = tupleStreamOperator;
		this.tupleStreamSource = tupleStreamOperator.iterator();
		this.indexReader = indexReader;
	}

	/**
	 * Close all iterators
	 */
	@Override
	public void close() {
		CloseableHelper.closeWithoutException(tupleStreamOperator);
	}
	
	/**
	 * Get the query processing result
	 * @return
	 */
	public Iterator<JoinedTuple> iterator() {
		
		return new Iterator<JoinedTuple>() {
			
			private JoinedTuple tupleFromStreamSource = null;
			
			private Iterator<JoinedTuple> candidatesForCurrentTuple = null;
			
			private JoinedTuple nextTuple = null;

			@Override
			public boolean hasNext() {
				
				while(nextTuple == null) {					
					// Join partner exhausted, try next tuple
					while(candidatesForCurrentTuple == null || ! candidatesForCurrentTuple.hasNext()) {						
						// Fetch next tuple from stream source
						if(! tupleStreamSource.hasNext()) { 
							return false;
						} else {
							// Start a new index scan for the next steam source tuple bounding box
							tupleFromStreamSource = tupleStreamSource.next();
							CloseableHelper.closeWithoutException(indexReader);
							indexReader.setBoundingBox(tupleFromStreamSource.getBoundingBox());
							candidatesForCurrentTuple = indexReader.iterator();							
						} 
					}
					
					final Tuple nextCandidateTuple = candidatesForCurrentTuple.next().convertToSingleTupleIfPossible();
					assert (nextCandidateTuple.getBoundingBox().overlaps(tupleFromStreamSource.getBoundingBox())) : "Wrong join, no overlap";
					nextTuple = buildNextJoinedTuple(nextCandidateTuple);
				}
								
				return nextTuple != null;
			}

			/**
			 * Build the next joined tuple
			 * @param nextCandidateTuple
			 * @return
			 */
			private JoinedTuple buildNextJoinedTuple(final Tuple nextCandidateTuple) {
				final List<String> tupleStoreNames = tupleFromStreamSource.getTupleStoreNames();
				final List<Tuple> tuples = tupleFromStreamSource.getTuples();
				
				tupleStoreNames.add(indexReader.getTupleStoreName().getFullnameWithoutPrefix());
				tuples.add(nextCandidateTuple);

				return new JoinedTuple(tuples, tupleStoreNames);
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
		};	
	}
}
