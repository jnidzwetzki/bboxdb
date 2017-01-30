package org.bboxdb.storage.queryprocessor.queryplan;

import java.util.Iterator;

import org.bboxdb.storage.ReadOnlyTupleStorage;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.queryprocessor.datasource.DataSource;
import org.bboxdb.storage.queryprocessor.datasource.SpatialIndexDataSource;

public class BoundingBoxQueryPlan implements QueryPlan {

	/**
	 * The bounding box for the query
	 */
	protected final BoundingBox boundingBox;
	
	public BoundingBoxQueryPlan(final BoundingBox boundingBox) {
		this.boundingBox = boundingBox;
	}

	@Override
	public Iterator<Tuple> execute(ReadOnlyTupleStorage readOnlyTupleStorage) {
		final DataSource dataSource = new SpatialIndexDataSource(readOnlyTupleStorage, boundingBox);
		
		return dataSource.iterator();
	}

}
