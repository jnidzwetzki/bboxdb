/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.query;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.transformation.BoundingBoxFilterTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByAmountTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByFactorTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByWGS84Transformation;
import org.bboxdb.query.transformation.KeyFilterTransformation;
import org.bboxdb.query.transformation.TupleTransformation;

public class QueryPlanBuilder {
	
	/**
	 * The query UUID
	 */
	private final String queryUUID;
	
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
	 * The join filters
	 */
	private final List<UserDefinedFilterDefinition> streamFilters;
	
	/**
	 * The join filters
	 */
	private final List<UserDefinedFilterDefinition> joinFilters;
	
	/**
	 * Report positive or negative matches
	 */
	private boolean reportPositiveMatches;
	
	/**
	 * This query should receive watermarks
	 */
	private boolean receiveWatermarks;
	
	/**
	 * This query should receive invalidations
	 */
	private boolean receiveInvalidations;
	
	/**
	 * Invalidate state after n watermarks 
	 */
	private long invalidateStateAfterWatermarks;

	public QueryPlanBuilder(final String tablename) {
		this.queryUUID = UUID.randomUUID().toString();
		this.streamTable = tablename;
		this.streamTupleTransformation = new ArrayList<>();
		this.storedTupleTransformation = new ArrayList<>();
		this.streamFilters = new ArrayList<>();
		this.joinFilters = new ArrayList<>();
		this.reportPositiveMatches = true;
		this.receiveInvalidations = false;
		this.receiveWatermarks = false;
		this.invalidateStateAfterWatermarks = 0;
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
	public QueryPlanBuilder forAllNewTuplesInSpace(final Hyperrectangle hyperrectangle) {
		this.queryRegion = hyperrectangle;
		return this;
	}
	
	/**
	 * Compare the tuples with a static region
	 * @param region
	 * @return
	 */
	public QueryPlanBuilder compareWithStaticSpace(final Hyperrectangle regionConst) {
		this.regionConst = regionConst;
		return this;
	}
	
	/**
	 * Compare with the given table
	 * @param table
	 * @return
	 */
	public QueryPlanBuilder spatialJoinWithTable(final String table) {
		this.joinTable = table;
		return this;
	}
	
	/**
	 * Report only positive matches
	 * @return
	 */
	public QueryPlanBuilder reportPositiveMatches() {
		this.reportPositiveMatches = true;
		return this;
	}
	
	/**
	 * Report only negative matches
	 * @return
	 */
	public QueryPlanBuilder reportNegativeMatches() {
		this.reportPositiveMatches = false;
		return this;
	}
	
	/**
	 * Receive watermarks (if available)
	 * @return
	 */
	public QueryPlanBuilder receiveWatermarks() {
		this.receiveWatermarks = true;
		return this;
	}
	
	/**
	 * Don't receive watermarks
	 * @return
	 */
	public QueryPlanBuilder dontReceiveWatermarks() {
		this.receiveWatermarks = false;
		return this;
	}
	
	/**
	 * Receive invalidations
	 * @return
	 */
	public QueryPlanBuilder receiveInvalidations() {
		this.receiveInvalidations = true;
		return this;
	}
	
	/**
	 * Don't receive invalidations
	 * @return
	 */
	public QueryPlanBuilder dontReceiveInvalidations() {
		this.receiveInvalidations = false;
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
	public QueryPlanBuilder enlargeStreamTupleBoundBoxByValue(final double amount) {
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
	public QueryPlanBuilder filterStoredTupleByBoundingBox(final Hyperrectangle bbox) {
		storedTupleTransformation.add(new BoundingBoxFilterTransformation(bbox));
		return this;
	}
	
	/**
	 * Add a join filter
	 * @param userDefinedFilter
	 * @return
	 */
	public QueryPlanBuilder addJoinFilter(final UserDefinedFilterDefinition userDefinedFilter) {
		joinFilters.add(userDefinedFilter);
		return this;
	}
	
	/**
	 * Add a range filter
	 * @param userDefinedFilter
	 * @return 
	 * @return
	 */
	public QueryPlanBuilder addStreamFilter(UserDefinedFilterDefinition userDefinedFilter) {
		streamFilters.add(userDefinedFilter);
		return this;
	}
	
	/**
	 * Invalidate the state of the query after n watermarks
	 */
	public QueryPlanBuilder invalidateStateAfterWartermarks(final long watermarks) {
		this.invalidateStateAfterWatermarks = watermarks;
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
			return new ContinuousRangeQueryPlan(queryUUID, streamTable, streamTupleTransformation, 
					queryRegion, regionConst, reportPositiveMatches, streamFilters, 
					receiveWatermarks, receiveInvalidations, invalidateStateAfterWatermarks);
		}
		
		if(joinTable != null) {
			
			if(reportPositiveMatches == false) {
				throw new IllegalArgumentException("Unable to create continous join queries with negative matches");
			}
			
			return new ContinuousSpatialJoinQueryPlan(queryUUID, streamTable, joinTable, 
					streamTupleTransformation, queryRegion, storedTupleTransformation,
					streamFilters, joinFilters, receiveWatermarks, receiveInvalidations,
					invalidateStateAfterWatermarks);
		}
		
		throw new IllegalArgumentException("Join table or const region need to be set");
	}

}
