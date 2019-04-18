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
package org.bboxdb.network.query;

import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.transformation.BoundingBoxFilterTransformation;
import org.bboxdb.network.query.transformation.EnlargeBoundingBoxByAmountTransformation;
import org.bboxdb.network.query.transformation.EnlargeBoundingBoxByFactorTransformation;
import org.bboxdb.network.query.transformation.EnlargeBoundingBoxByWGS84Transformation;
import org.bboxdb.network.query.transformation.KeyFilterTransformation;
import org.bboxdb.network.query.transformation.TupleTransformation;

public class QueryPlanBuilder {
	
	/**
	 * The tablename
	 */
	private final String streamTable;
	
	/**
	 * The join table name
	 */
	private String joinTable;
	
	/**
	 * The query region
	 */
	private Hyperrectangle queryRegion;
	
	/**
	 * The region to compare with
	 */
	private Hyperrectangle regionConst;
	
	/**
	 * Transformations of the stream tuple
	 */
	private final List<TupleTransformation> streamTupleTransformation;
	
	/**
	 * Transformations of the stored tuple
	 */
	private final List<TupleTransformation> storedTupleTransformation;
	
	/**
	 * Report positive or negative matches
	 */
	private boolean reportPositiveMatches;

	public QueryPlanBuilder(final String tablename) {
		this.streamTable = tablename;
		this.streamTupleTransformation = new ArrayList<>();
		this.storedTupleTransformation = new ArrayList<>();
		this.reportPositiveMatches = true;
		this.queryRegion = Hyperrectangle.FULL_SPACE;
	}

	/**
	 *  Create a new query plan builder
	 * @param tablename
	 * @param region
	 * @return
	 */
	public static QueryPlanBuilder createQueryOnTable(final String tablename) {
		return new QueryPlanBuilder(tablename);
	}
	
	/**
	 * Execute the query on region
	 * @param values
	 * @return
	 */
	public QueryPlanBuilder forAllNewTuplesStoredInRegion(final Double... values) {
		this.queryRegion = new Hyperrectangle(values);
		return this;
	}
	
	/**
	 * Compare the tuples with a static region
	 * @param region
	 * @return
	 */
	public QueryPlanBuilder compareWithStaticRegion(final Double... values) {
		this.regionConst = new Hyperrectangle(values);
		return this;
	}
	
	public QueryPlanBuilder compareWithTable(final String table) {
		this.joinTable = table;
		return this;
	}
	
	/**
	 * Report only positive matches
	 * @param reportPositiveMatches
	 * @return
	 */
	public QueryPlanBuilder reportPositiveMatches() {
		this.reportPositiveMatches = true;
		return this;
	}
	
	/**
	 * Report only negative matches
	 * @param reportPositiveMatches
	 * @return
	 */
	public QueryPlanBuilder reportNegativeMatches() {
		this.reportPositiveMatches = false;
		return this;
	}
	
	/**
	 * Filter the stream tuples by key
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder filterStreamTupleByKey(final String key) {
		streamTupleTransformation.add(new KeyFilterTransformation(key));
		return this;
	}
	
	/**
	 * Filter the stream tuples by bounding box
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder filterStreamTupleByBoundingBox(final Double... values) {
		final Hyperrectangle bbox = new Hyperrectangle(values);
		streamTupleTransformation.add(new BoundingBoxFilterTransformation(bbox));
		return this;
	}
	
	/**
	 * Enlarge bounding box of the stream by amount
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder enlargeStreamTupleBoundBoxByAmount(final double amount) {
		streamTupleTransformation.add(new EnlargeBoundingBoxByAmountTransformation(amount));
		return this;
	}
	
	/**
	 * Enlarge bounding box of the stream by factor
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder enlargeStreamTupleBoundBoxByFactor(final double factor) {
		streamTupleTransformation.add(new EnlargeBoundingBoxByFactorTransformation(factor));
		return this;
	}
	
	/**
	 * Enlarge bounding box of the stored tuple by factor
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder enlargeStreamTupleBoundBoxByWGS84Meter(final double meterLat, final double meterLong) {
		streamTupleTransformation.add(new EnlargeBoundingBoxByWGS84Transformation(meterLat, meterLong));
		return this;
	}
	
	/**
	 * Filter the stored tuples by key
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder filterStoredTupleByKey(final String key) {
		storedTupleTransformation.add(new KeyFilterTransformation(key));
		return this;
	}
	
	/**
	 * Filter the stored tuples by bounding box
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder filterStoredTupleByBoundingBox(final Double... values) {
		final Hyperrectangle bbox = new Hyperrectangle(values);
		storedTupleTransformation.add(new BoundingBoxFilterTransformation(bbox));
		return this;
	}
	
	/**
	 * Enlarge bounding box of the stored tuple by amount
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder enlargeStoredTupleBoundBoxByAmount(final double amount) {
		storedTupleTransformation.add(new EnlargeBoundingBoxByAmountTransformation(amount));
		return this;
	}
	
	/**
	 * Enlarge bounding box of the stored tuple by factor
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder enlargeStoredTupleBoundBoxByFactor(final double factor) {
		storedTupleTransformation.add(new EnlargeBoundingBoxByFactorTransformation(factor));
		return this;
	}
	
	/**
	 * Enlarge bounding box of the stored tuple by factor
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder enlargeStoredTupleBoundBoxByWGS84Meter(final double meterLat, final double meterLong) {
		storedTupleTransformation.add(new EnlargeBoundingBoxByWGS84Transformation(meterLat, meterLong));
		return this;
	}
	
	/**
	 * Build the query plan
	 * 
	 * Query region is per default the complete space
	 * 
	 * @return
	 */
	public ContinuousQueryPlan build() {
		
		if(regionConst != null && joinTable != null) {
			throw new IllegalArgumentException("Unable to construct query plan with "
					+ "const hyperrectangle and join table");
		}
		
		if(regionConst != null) {
			return new ContinuousConstQueryPlan(streamTable, streamTupleTransformation, 
					queryRegion, regionConst, reportPositiveMatches);
		}
		
		if(joinTable != null) {
			return new ContinuousTableQueryPlan(streamTable, joinTable, streamTupleTransformation, queryRegion, 
					storedTupleTransformation, reportPositiveMatches);
		}
		
		throw new IllegalArgumentException("Join table or const region need to be set");
	}

}
