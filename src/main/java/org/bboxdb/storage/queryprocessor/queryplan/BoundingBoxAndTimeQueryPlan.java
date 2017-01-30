package org.bboxdb.storage.queryprocessor.queryplan;

import java.util.Iterator;

import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.datasource.DataSource;
import org.bboxdb.storage.queryprocessor.datasource.SpatialIndexDataSource;
import org.bboxdb.storage.queryprocessor.predicate.NewerAsTimePredicate;
import org.bboxdb.storage.queryprocessor.predicate.PredicateFilterIterator;

public class BoundingBoxAndTimeQueryPlan implements QueryPlan {

	/**
	 * The bounding box for the query
	 */
	protected final BoundingBox boundingBox;
	
	/**
	 * The timestamp for the query
	 * 
	 * @param boundingBox
	 * @param timestamp
	 */
	protected final long timestamp;
	
	public BoundingBoxAndTimeQueryPlan(final BoundingBox boundingBox, final long timestamp) {
		this.boundingBox = boundingBox;
		this.timestamp = timestamp;
	}

	@Override
	public Iterator<Tuple> execute(ReadOnlyTupleStorage readOnlyTupleStorage) {
		final DataSource dataSource = new SpatialIndexDataSource(readOnlyTupleStorage, boundingBox);
		
		final NewerAsTimePredicate predicate = new NewerAsTimePredicate(timestamp);
		
		final PredicateFilterIterator predicateFilterIterator 
			= new PredicateFilterIterator(dataSource.iterator(), predicate);
		
		return predicateFilterIterator;
	}
}
