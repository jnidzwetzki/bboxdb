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
import java.util.Objects;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.transformation.BoundingBoxFilterTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByAmountTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByFactorTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByWGS84Transformation;
import org.bboxdb.query.transformation.KeyFilterTransformation;
import org.bboxdb.query.transformation.TupleTransformation;
import org.bboxdb.query.transformation.UserDefinedFilterTransformation;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ContinuousQueryPlanSerializer {
	
	/**
	 * Query plan key and value
	 */
	private static final String TYPE_KEY = "type";
	private static final String TYPE_VALUE = "query-plan";
	private static final String QUERY_UUID = "query-uuid";
	

	/**
	 * Query type key and values
	 */
	private static final String QUERY_TYPE_KEY = "query-type";
	private static final String QUERY_TYPE_JOIN_VALUE = "join-query";
	private static final String QUERY_TYPE_RANGE_VALUE = "range-query";
	
	/**
	 * Misc keys
	 */
	private static final String TABLE_TRANSFORMATIONS_KEY = "table-transformations";
	private static final String STREAM_TRANSFORMATIONS_KEY = "stream-transformations";
	private static final String STREAM_FILTER_KEY = "stream-filter";
	private static final String JOIN_FILTER_KEY = "join-filter";
	
	private static final String REPORT_POSITIVE_KEY = "report-positive";
	private static final String REPORT_WATERMARKS_KEY = "report-watermarks";
	private static final String REPORT_INVALIDATIONS_KEY = "report-invalidations";
	private static final String INVALIDATE_STATE_AFTER = "invalidate-state-after";

	private static final String STREAM_TABLE_KEY = "stream-table";
	private static final String JOIN_TABLE_KEY = "join-table";
	private static final String QUERY_RANGE_KEY = "query-range";
	private static final String COMPARE_RECTANGLE_KEY = "compare-rectangle";
	
	/**
	 * Transformation type
	 */
	private static final String TRANSFORMATION_NAME_KEY = "name";
	private static final String TRANSFORMATION_KEY_FILTER_VALUE = "key-filter";
	private static final String TRANSFORMATION_BBOX_ENLARGE_AMOUNT_VALUE = "bbox-enlarge-by-amount";
	private static final String TRANSFORMATION_BBOX_ENLARGE_FACTOR_VALUE = "bbox-enlarge-by-factor";
	private static final String TRANSFORMATION_BBOX_ENLARGE_WGS84_VALUE = "bbox-enlarge-by-wgs84";
	private static final String TRANSFORMATION_BBOX_FILTER_VALUE = "bbox-filter";
	private static final String TRANSFORMATION_USER_DEFINED_FILTER = "user-defined-filter";

	
	/**
	 * Transformation value
	 */
	private static final String TRANSFORMATION_VALUE_KEY = "value";
	
	/**
	 * User defined filter - class
	 */
	private static final String USER_DEFINED_FILTER_CLASS = "filter-class";
	
	/**
	 * User defined filter - value
	 */
	private static final String USER_DEFINED_FILTER_VALUE = "filter-value";
	
	
	
	
	/**
	 * Serialize to JSON
	 * @param queryPlan
	 * @return
	 */
	public static String toJSON(final ContinuousQueryPlan queryPlan) {
		final JSONObject json = new JSONObject();
		json.put(TYPE_KEY, TYPE_VALUE);
		json.put(QUERY_UUID, queryPlan.getQueryUUID());
		
		if(queryPlan instanceof ContinuousRangeQueryPlan) {
			json.put(QUERY_TYPE_KEY, QUERY_TYPE_RANGE_VALUE);
			
			final ContinuousRangeQueryPlan constQueryPlan = (ContinuousRangeQueryPlan) queryPlan;
			json.put(COMPARE_RECTANGLE_KEY, constQueryPlan.getCompareRectangle().toCompactString());
			json.put(REPORT_POSITIVE_KEY, constQueryPlan.isReportPositive());
		} else if(queryPlan instanceof ContinuousSpatialJoinQueryPlan) {
			json.put(QUERY_TYPE_KEY, QUERY_TYPE_JOIN_VALUE);

			final ContinuousSpatialJoinQueryPlan tableQueryPlan = (ContinuousSpatialJoinQueryPlan) queryPlan;
			
			final List<TupleTransformation> tableTransformations = tableQueryPlan.getTableTransformation();
			final JSONArray tableTransformationsJSON = writeTransformationsToJSON(json, tableTransformations);
			json.put(TABLE_TRANSFORMATIONS_KEY, tableTransformationsJSON);
			
			final List<UserDefinedFilterDefinition> joinTransformations = tableQueryPlan.getAfterJoinFilter();
			final JSONArray joinFilterJSON = writeFilterToJSON(joinTransformations);
			json.put(JOIN_FILTER_KEY, joinFilterJSON);
			
			json.put(JOIN_TABLE_KEY, tableQueryPlan.getJoinTable());					
		} else {
			throw new IllegalArgumentException("Unknown query type: " + queryPlan);
		}
		
		json.put(QUERY_RANGE_KEY, queryPlan.getQueryRange().toCompactString());
		json.put(STREAM_TABLE_KEY, queryPlan.getStreamTable());
		json.put(REPORT_WATERMARKS_KEY, queryPlan.isReceiveWatermarks());
		json.put(REPORT_INVALIDATIONS_KEY, queryPlan.isReceiveInvalidations());
		json.put(INVALIDATE_STATE_AFTER, queryPlan.getInvalidateStateAfterWatermarks());

		final List<UserDefinedFilterDefinition> streamFilters = queryPlan.getStreamFilters();
		final JSONArray streamFilterJSON = writeFilterToJSON(streamFilters);
		json.put(STREAM_FILTER_KEY, streamFilterJSON);
		
		final List<TupleTransformation> transformations = queryPlan.getStreamTransformation();
		final JSONArray streamTransformations = writeTransformationsToJSON(json, transformations);
		json.put(STREAM_TRANSFORMATIONS_KEY, streamTransformations);

		return json.toString();
	}

	/**
	 * Write the transformations into a JSON array
	 * @param json
	 * @param transformations
	 * @return 
	 */
	private static JSONArray writeTransformationsToJSON(final JSONObject json,
			final List<TupleTransformation> transformations) {
		
		final JSONArray transfomationArray = new JSONArray();
		
		for(final TupleTransformation transformation : transformations) {
			final JSONObject transformationJSON = new JSONObject();
			
			if(transformation instanceof BoundingBoxFilterTransformation) {
				transformationJSON.put(TRANSFORMATION_NAME_KEY, TRANSFORMATION_BBOX_FILTER_VALUE);
			} else if(transformation instanceof EnlargeBoundingBoxByAmountTransformation) {
				transformationJSON.put(TRANSFORMATION_NAME_KEY, TRANSFORMATION_BBOX_ENLARGE_AMOUNT_VALUE);
			} else if(transformation instanceof EnlargeBoundingBoxByFactorTransformation) {
				transformationJSON.put(TRANSFORMATION_NAME_KEY, TRANSFORMATION_BBOX_ENLARGE_FACTOR_VALUE);
			} else if(transformation instanceof KeyFilterTransformation) {
				transformationJSON.put(TRANSFORMATION_NAME_KEY, TRANSFORMATION_KEY_FILTER_VALUE);
			} else if(transformation instanceof EnlargeBoundingBoxByWGS84Transformation) {
				transformationJSON.put(TRANSFORMATION_NAME_KEY, TRANSFORMATION_BBOX_ENLARGE_WGS84_VALUE);
			} else if(transformation instanceof UserDefinedFilterTransformation) {
				transformationJSON.put(TRANSFORMATION_NAME_KEY, TRANSFORMATION_USER_DEFINED_FILTER);
			} else {
				throw new IllegalArgumentException("Unable to serialize type: " + transformation);
			}
			
			transformationJSON.put(TRANSFORMATION_VALUE_KEY, transformation.getSerializedData());
			
			transfomationArray.put(transformationJSON);
		}
		
		return transfomationArray;
	}
	
	/**
	 * Write a list with transformations to JSON
	 * @param json
	 * @param joinTransformations
	 * @return
	 */
	private static JSONArray writeFilterToJSON(final List<UserDefinedFilterDefinition> filters) {
		
		final JSONArray transfomationArray = new JSONArray();
		
		for(final UserDefinedFilterDefinition filter : filters) {
			final JSONObject transformationJSON = new JSONObject();
			
			transformationJSON.put(USER_DEFINED_FILTER_CLASS, filter.getUserDefinedFilterClass());
			transformationJSON.put(USER_DEFINED_FILTER_VALUE, filter.getUserDefinedFilterValue());
			
			transfomationArray.put(transformationJSON);
		}

		return transfomationArray;
	}
	
	/**
	 * Deserialize query plan
	 * @param json
	 * @return
	 * @throws BBoxDBException 
	 */
	public static ContinuousQueryPlan fromJSON(final String jsonString) throws BBoxDBException {
		
		Objects.requireNonNull(jsonString);
		
		try {
			final JSONObject json = new JSONObject(jsonString);
			
			if(! json.getString(TYPE_KEY).equals(TYPE_VALUE)) {
				throw new BBoxDBException("JSON does not contain a valid query plan: " + jsonString);
			}
			
			final String queryUUID = json.getString(QUERY_UUID);
			final String queryType = json.getString(QUERY_TYPE_KEY);
			final String streamTable = json.getString(STREAM_TABLE_KEY);
			final Hyperrectangle queryRectangle = Hyperrectangle.fromString(json.getString(QUERY_RANGE_KEY));
			
			final List<TupleTransformation> streamTransformation 
				= decodeTransformation(json, STREAM_TRANSFORMATIONS_KEY);
			
			final List<UserDefinedFilterDefinition> streamFilters = decodeFilters(json, STREAM_FILTER_KEY);

			final boolean receiveWatermarks = json.getBoolean(REPORT_WATERMARKS_KEY);
			final boolean receiveInvalidations = json.getBoolean(REPORT_INVALIDATIONS_KEY);
			final long invalidateStateAfterWatermarks = json.getLong(INVALIDATE_STATE_AFTER);
			
			switch(queryType) {
			case QUERY_TYPE_RANGE_VALUE:
				final boolean reportPositiveNegative = json.getBoolean(REPORT_POSITIVE_KEY);

				final Hyperrectangle compareRectangle = Hyperrectangle.fromString(json.getString(COMPARE_RECTANGLE_KEY));
				
				final ContinuousRangeQueryPlan constQuery = new ContinuousRangeQueryPlan(queryUUID, streamTable, 
						streamTransformation, queryRectangle, compareRectangle, 
						reportPositiveNegative, streamFilters, 
						receiveWatermarks, receiveInvalidations, invalidateStateAfterWatermarks);
				
				return constQuery;
			case QUERY_TYPE_JOIN_VALUE:				
				final List<TupleTransformation> tableTransformation 
					= decodeTransformation(json, TABLE_TRANSFORMATIONS_KEY);
				
				final List<UserDefinedFilterDefinition> joinFilters = decodeFilters(json, JOIN_FILTER_KEY);
				
				final String joinTable = json.getString(JOIN_TABLE_KEY);
				
				final ContinuousSpatialJoinQueryPlan tableQuery = new ContinuousSpatialJoinQueryPlan(queryUUID, streamTable, 
						joinTable, streamTransformation, queryRectangle, 
						tableTransformation, streamFilters, joinFilters, 
						receiveWatermarks, receiveInvalidations, invalidateStateAfterWatermarks);
		
				return tableQuery;
			default:
				throw new BBoxDBException("Unknown query type: " + queryType);
			}
			
		} catch(JSONException e) {
			throw new BBoxDBException("Unable to handle json: " + jsonString, e);
		}		
	}
	
	/**
	 * Decode the given transformations
	 * @param json
	 * @param key
	 * @return
	 * @throws BBoxDBException 
	 */
	private static List<TupleTransformation> decodeTransformation(final JSONObject json, final String key) 
			throws BBoxDBException {
		
		final List<TupleTransformation> transformations = new ArrayList<>();
		
		final JSONArray transformationArray = json.getJSONArray(key);
		
		for(int i = 0; i < transformationArray.length(); i++) {
			final JSONObject transformationObject = transformationArray.getJSONObject(i);
			final String transformationType = transformationObject.getString(TRANSFORMATION_NAME_KEY);
			final String transformationValue = transformationObject.getString(TRANSFORMATION_VALUE_KEY);
			
			try {
				TupleTransformation transformation;
				
				switch(transformationType) {
					case TRANSFORMATION_BBOX_ENLARGE_AMOUNT_VALUE:
						transformation = new EnlargeBoundingBoxByAmountTransformation(transformationValue);
						break;
					case TRANSFORMATION_BBOX_ENLARGE_FACTOR_VALUE:
						transformation = new EnlargeBoundingBoxByFactorTransformation(transformationValue);
						break;
					case TRANSFORMATION_BBOX_FILTER_VALUE:
						transformation = new BoundingBoxFilterTransformation(transformationValue);
						break;
					case TRANSFORMATION_KEY_FILTER_VALUE:
						transformation = new KeyFilterTransformation(transformationValue);
						break;
					case TRANSFORMATION_BBOX_ENLARGE_WGS84_VALUE:
						transformation = new EnlargeBoundingBoxByWGS84Transformation(transformationValue);
						break;
					case TRANSFORMATION_USER_DEFINED_FILTER:
						transformation = new UserDefinedFilterTransformation(transformationValue);
						break;
						
					default:
						throw new BBoxDBException("Unkown transformation type: " + transformationType);
				}
				
				transformations.add(transformation);
			} catch (InputParseException e) {
				throw new BBoxDBException(e);
			}
		}
		
		return transformations;
	}
	
	/**
	 * Decode the given user defined filters
	 * @param json
	 * @param key
	 * @return
	 * @throws BBoxDBException 
	 */
	private static List<UserDefinedFilterDefinition> decodeFilters(final JSONObject json, final String key) 
			throws BBoxDBException {
		
		final List<UserDefinedFilterDefinition> filters = new ArrayList<>();
		
		final JSONArray filtersArray = json.getJSONArray(key);
		
		for(int i = 0; i < filtersArray.length(); i++) {
			final JSONObject transformationObject = filtersArray.getJSONObject(i);
			
			final String filterClass = transformationObject.getString(USER_DEFINED_FILTER_CLASS);
			final String filterValue = transformationObject.getString(USER_DEFINED_FILTER_VALUE);
			
			final UserDefinedFilterDefinition userDefinedFilterDefinition 
				= new UserDefinedFilterDefinition(filterClass, filterValue);
			
			filters.add(userDefinedFilterDefinition);
		}
		
		return filters;
	}
	
	
}
