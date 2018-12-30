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
import org.bboxdb.network.query.transformation.KeyFilterTransformation;
import org.bboxdb.network.query.transformation.TupleTransformation;

public class QueryPlanBuilder {
	
	/**
	 * The tablename
	 */
	private final String tablename;
	
	/**
	 * The query region
	 */
	private final Hyperrectangle queryRegion;
	
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

	public QueryPlanBuilder(final String tablename, final Hyperrectangle region) {
		this.tablename = tablename;
		this.queryRegion = region;
		this.streamTupleTransformation = new ArrayList<>();
		this.storedTupleTransformation = new ArrayList<>();
		this.reportPositiveMatches = true;
	}

	/**
	 *  Create a new query plan builder
	 * @param tablename
	 * @param region
	 * @return
	 */
	public static QueryPlanBuilder createQueryOnTableAndRegion(final String tablename, 
			final Hyperrectangle region) {
		
		return new QueryPlanBuilder(tablename, region);
	}
	
	/**
	 * Compare the tuples with a static region
	 * @param region
	 * @return
	 */
	public QueryPlanBuilder compareWithStaticRegion(final Hyperrectangle region) {
		this.regionConst = region;
		return this;
	}
	
	/**
	 * Report positive or negative matches
	 * @param reportPositiveMatches
	 * @return
	 */
	public QueryPlanBuilder reportPositiveMatches(final boolean reportPositiveMatches) {
		this.reportPositiveMatches = reportPositiveMatches;
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
	public QueryPlanBuilder filterStreamTupleByBoundingBox(final Hyperrectangle bbox) {
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
	 * Filter the stored tuples by key
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder filterStoredTupleByKey(final String key) {
		storedTupleTransformation.add(new KeyFilterTransformation(key));
		return this;
	}
	
	/**
	 * Filter the strored tuples by bounding box
	 * @param key
	 * @return
	 */
	public QueryPlanBuilder filterStoredTupleByBoundingBox(final Hyperrectangle bbox) {
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
	 * Build the query plan
	 * @return
	 */
	public ContinuousQueryPlan build() {
		if(regionConst != null && ! storedTupleTransformation.isEmpty()) {
			throw new IllegalArgumentException("Unable to construct query plan with "
					+ "const hyperrectangle and tuple store transformations");
		}
		
		if(regionConst != null) {
			return new ContinuousConstQueryPlan(tablename, streamTupleTransformation, 
					queryRegion, regionConst, reportPositiveMatches);
		}
		
		
		return new ContinuousTableQueryPlan(tablename, streamTupleTransformation, queryRegion, 
				storedTupleTransformation, reportPositiveMatches);
	}

}
