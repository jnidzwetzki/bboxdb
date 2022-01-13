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
package org.bboxdb.test.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.ContinuousQueryPlanSerializer;
import org.bboxdb.query.ContinuousRangeQueryPlan;
import org.bboxdb.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.transformation.BoundingBoxFilterTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByAmountTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByFactorTransformation;
import org.bboxdb.query.transformation.EnlargeBoundingBoxByWGS84Transformation;
import org.bboxdb.query.transformation.KeyFilterTransformation;
import org.junit.Assert;
import org.junit.Test;

public class TestContinuousQueryPlan {

	@Test(timeout=60_000, expected=NullPointerException.class)
	public void testInvliadQueryPlan1() throws BBoxDBException {
		ContinuousQueryPlanSerializer.fromJSON(null);
	}
	
	@Test(timeout=60_000, expected=BBoxDBException.class)
	public void testInvliadQueryPlan2() throws BBoxDBException {
		ContinuousQueryPlanSerializer.fromJSON("");
	}
	
	@Test(timeout=60_000, expected=BBoxDBException.class)
	public void testInvliadQueryPlan3() throws BBoxDBException {
		ContinuousQueryPlanSerializer.fromJSON("{{{");
	}
	
	@Test(timeout=60_000, expected=BBoxDBException.class)
	public void testInvliadQueryPlan4() throws BBoxDBException {
		ContinuousQueryPlanSerializer.fromJSON("{{{}}}");
	}
	
	@Test(timeout=60_000, expected=BBoxDBException.class)
	public void testInvliadQueryPlan5() throws BBoxDBException {
		ContinuousQueryPlanSerializer.fromJSON("{\"abc\":2}");
	}
	
	@Test(timeout=60_000, expected=BBoxDBException.class)
	public void testInvliadQueryPlan6() throws BBoxDBException {
		ContinuousQueryPlanSerializer.fromJSON("{\"query-type\":2, \"type\":\"query-plan\"}");
	}
	
	@Test(timeout=60_000, expected=BBoxDBException.class)
	public void testInvalidTransformationType() throws BBoxDBException {
		// Invalid transformation type "bbox-filter1234"
		final String json = "{\"query-range\":\"[]\",\"query-type\":\"const-query\",\"compare-rectangle\":\"[[12.0,13.0]:[14.0,15.0]]\",\"report-positive\":false,\"stream-transformations\":[{\"name\":\"bbox-filter1234\",\"value\":\"[[12.0,13.0]:[14.0,15.0]]\"}],\"type\":\"query-plan\",\"table\":\"abc\"}";
		ContinuousQueryPlanSerializer.fromJSON(json);
	}
	
	@Test(timeout=60_000)
	public void testConstQuery1() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousRangeQueryPlan(UUID.randomUUID().toString(),
				"abc", 
				new ArrayList<>(), 
				Hyperrectangle.FULL_SPACE, 
				new Hyperrectangle(12d, 13d, 14d, 15d), false, new ArrayList<>(), false, false, 0);
		
		serializeAndDeserialize(continuousQueryPlan);
	}
	
	@Test(timeout=60_000)
	public void testConstQuery2() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousRangeQueryPlan(UUID.randomUUID().toString(),
				"testtable", 
				new ArrayList<>(), 
				new Hyperrectangle(12d, 13d, 14d, 15d), 
				new Hyperrectangle(12d, 13d, 14d, 15d), true, new ArrayList<>(), false, true, 1);
		
		serializeAndDeserialize(continuousQueryPlan);
	}
	

	@Test(timeout=60_000)
	public void testConstQuery3() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousRangeQueryPlan(UUID.randomUUID().toString(),
				"testtable", 
				Arrays.asList(new BoundingBoxFilterTransformation(new Hyperrectangle(12d, 13d, 14d, 15d))), 
				new Hyperrectangle(12d, 13d, 14d, 15d), 
				new Hyperrectangle(12d, 13d, 14d, 15d), true, new ArrayList<>(), true, false, 10);
		
		serializeAndDeserialize(continuousQueryPlan);
	}
	
	@Test(timeout=60_000)
	public void testConstQuery4() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousRangeQueryPlan(UUID.randomUUID().toString(),
				"testtable", 
				Arrays.asList(
						new BoundingBoxFilterTransformation(new Hyperrectangle(12d, 13d, 14d, 15d)),
						new KeyFilterTransformation("abcd")), 
				new Hyperrectangle(12d, 13d, 14d, 15d), 
				new Hyperrectangle(12d, 13d, 14d, 15d), true, new ArrayList<>(), false, false, 1);
		
		serializeAndDeserialize(continuousQueryPlan);
	}
	
	@Test(timeout=60_000)
	public void testConstQuery5() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousRangeQueryPlan(UUID.randomUUID().toString(),
				"testtable", 
				Arrays.asList(
						new BoundingBoxFilterTransformation(new Hyperrectangle(12d, 13d, 14d, 15d)),
						new KeyFilterTransformation("abcd"),
						new EnlargeBoundingBoxByAmountTransformation(4)), 
				new Hyperrectangle(12d, 13d, 14d, 15d), 
				new Hyperrectangle(12d, 13d, 14d, 15d), true, new ArrayList<>(), true, true, 0);
		
		serializeAndDeserialize(continuousQueryPlan);
	}
	
	@Test(timeout=60_000)
	public void testTableQuery1() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousSpatialJoinQueryPlan(UUID.randomUUID().toString(),
				"mytable", "mytable",
				new ArrayList<>(), 
				new Hyperrectangle(12d, 13d, 14d, 15d), 
				new ArrayList<>(), 
				new ArrayList<>(),
				new ArrayList<>(), false, false, 0);
		
		serializeAndDeserialize(continuousQueryPlan);
	}
	
	@Test(timeout=60_000)
	public void testTableQuery2() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousSpatialJoinQueryPlan(UUID.randomUUID().toString(),
				"mytable", "mytable",
				new ArrayList<>(), 
				new Hyperrectangle(12d, 13d, 14d, 15d), 
				Arrays.asList(
						new BoundingBoxFilterTransformation(new Hyperrectangle(12d, 13d, 14d, 15d)),
						new KeyFilterTransformation("abcd"),
						new EnlargeBoundingBoxByAmountTransformation(4),
						new EnlargeBoundingBoxByFactorTransformation(3)), 
				Arrays.asList(new UserDefinedFilterDefinition("abc", "def")),
				Arrays.asList(new UserDefinedFilterDefinition("xyyz", "456")), 
				false, false, 20
			);
		
		serializeAndDeserialize(continuousQueryPlan);
	}
	
	@Test(timeout=60_000)
	public void testTableQuery3() throws BBoxDBException {
		final ContinuousQueryPlan continuousQueryPlan = new ContinuousSpatialJoinQueryPlan(UUID.randomUUID().toString(),
				"mytable", "mytable",
				Arrays.asList(
						new BoundingBoxFilterTransformation(new Hyperrectangle(12d, 13d, 14d, 15d)),
						new KeyFilterTransformation("abcd"),
						new EnlargeBoundingBoxByWGS84Transformation(3.2, 1.0)
						), 
				new Hyperrectangle(12d, 13d, 14d, 15d), 
				Arrays.asList(
						new BoundingBoxFilterTransformation(new Hyperrectangle(12d, 13d, 14d, 15d)),
						new KeyFilterTransformation("abcd"),
						new EnlargeBoundingBoxByAmountTransformation(4),
						new EnlargeBoundingBoxByWGS84Transformation(3.2, 1.0)
						), 
				new ArrayList<>(),
				new ArrayList<>(), false, false, 20);
		
		serializeAndDeserialize(continuousQueryPlan);
	}

	/**
	 * Serialize and deserialize the given query plan
	 * @param continuousQueryPlan
	 * @throws BBoxDBException 
	 */
	private void serializeAndDeserialize(final ContinuousQueryPlan continuousQueryPlan) throws BBoxDBException {
		final String serializedQueryPlan = ContinuousQueryPlanSerializer.toJSON(continuousQueryPlan);
		Assert.assertNotNull(serializedQueryPlan);
				
		// Write JSON to stdout
		//System.out.println(serializedQueryPlan);
		
		final ContinuousQueryPlan deserializedQueryPlan = ContinuousQueryPlanSerializer.fromJSON(serializedQueryPlan);
		Assert.assertEquals(continuousQueryPlan, deserializedQueryPlan);
	}
	
}
