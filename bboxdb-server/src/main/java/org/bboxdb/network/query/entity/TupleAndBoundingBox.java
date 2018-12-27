package org.bboxdb.network.query.entity;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;

public class TupleAndBoundingBox {

	private final Tuple tuple;
	
	private final Hyperrectangle boundingBox;

	public TupleAndBoundingBox(final Tuple tuple, final Hyperrectangle boundingBox) {
		this.tuple = tuple;
		this.boundingBox = boundingBox;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public Hyperrectangle getBoundingBox() {
		return boundingBox;
	}

	@Override
	public String toString() {
		return "TupleAndBoundingBox [tuple=" + tuple + ", boundingBox=" + boundingBox + "]";
	}
	
}
