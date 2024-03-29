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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.ContinuousRangeQueryPlan;
import org.bboxdb.query.ContinuousSpatialJoinQueryPlan;
import org.bboxdb.query.QueryPlanBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestQueryPlanBuilder {
	
	@Test(timeout=60_000, expected=IllegalArgumentException.class)
	public void testInvalidPlan1() {
		QueryPlanBuilder
			.createQueryOnTable("table")
			.forAllNewTuplesInSpace(new Hyperrectangle(3d, 4d))
			.compareWithStaticSpace(new Hyperrectangle(2d, 4d))
			.spatialJoinWithTable("testtable")
			.build();	
	}
	
	@Test(timeout=60_000, expected=IllegalArgumentException.class)
	public void testInvalidPlan2() {
		QueryPlanBuilder
			.createQueryOnTable("table")
			.forAllNewTuplesInSpace(new Hyperrectangle(3d, 4d))
			.build();	
	}

	@Test(timeout=60_000)
	public void testConstPlan1() {
		final ContinuousQueryPlan queryPlan = QueryPlanBuilder
			.createQueryOnTable("table")
			.forAllNewTuplesInSpace(new Hyperrectangle(3d, 4d))
			.compareWithStaticSpace(new Hyperrectangle(2d, 4d))
			.build();
		
		Assert.assertEquals("table", queryPlan.getStreamTable());
		Assert.assertEquals(new Hyperrectangle(3d, 4d), queryPlan.getQueryRange());
		Assert.assertTrue(queryPlan.getStreamTransformation().isEmpty());
		
		final ContinuousRangeQueryPlan cqp = (ContinuousRangeQueryPlan) queryPlan;
		Assert.assertEquals(new Hyperrectangle(2d, 4d), cqp.getCompareRectangle());
	}
	
	@Test(timeout=60_000)
	public void testConstPlan2() {
		final ContinuousQueryPlan queryPlan = QueryPlanBuilder
			.createQueryOnTable("table")
			.forAllNewTuplesInSpace(new Hyperrectangle(3d, 4d))
			.compareWithStaticSpace(new Hyperrectangle(2d, 4d))
			.filterStreamTupleByBoundingBox(new Hyperrectangle(3d, 5d))
			.enlargeStreamTupleBoundBoxByValue(4)
			.enlargeStreamTupleBoundBoxByFactor(2)
			.invalidateStateAfterWartermarks(50)
			.build();
		
		Assert.assertEquals("table", queryPlan.getStreamTable());
		Assert.assertEquals(new Hyperrectangle(3d, 4d), queryPlan.getQueryRange());
		Assert.assertEquals(3, queryPlan.getStreamTransformation().size());
		
		final ContinuousRangeQueryPlan cqp = (ContinuousRangeQueryPlan) queryPlan;
		Assert.assertEquals(new Hyperrectangle(2d, 4d), cqp.getCompareRectangle());
		Assert.assertEquals(50, cqp.getInvalidateStateAfterWatermarks());
	}
	
	@Test(timeout=60_000, expected=IllegalArgumentException.class)
	public void testTablePlanException() {
		
		// Spatial join query with negative matches
		QueryPlanBuilder
			.createQueryOnTable("table")
			.forAllNewTuplesInSpace(new Hyperrectangle(3d, 4d))
			.filterStreamTupleByBoundingBox(new Hyperrectangle(1d, 5d))
			.enlargeStreamTupleBoundBoxByValue(4)
			.enlargeStreamTupleBoundBoxByFactor(2)
			.reportNegativeMatches()
			.spatialJoinWithTable("testtable")
			.build();
	}
	
	@Test(timeout=60_000)
	public void testTablePlan1() {
		final ContinuousQueryPlan queryPlan = QueryPlanBuilder
			.createQueryOnTable("table")
			.forAllNewTuplesInSpace(new Hyperrectangle(3d, 4d))
			.filterStreamTupleByBoundingBox(new Hyperrectangle(1d, 5d))
			.enlargeStreamTupleBoundBoxByValue(4)
			.enlargeStreamTupleBoundBoxByFactor(2)
			.spatialJoinWithTable("testtable")
			.build();
		
		Assert.assertEquals("table", queryPlan.getStreamTable());
		Assert.assertEquals(new Hyperrectangle(3d, 4d), queryPlan.getQueryRange());
		Assert.assertEquals(3, queryPlan.getStreamTransformation().size());
		
		final ContinuousSpatialJoinQueryPlan cqp = (ContinuousSpatialJoinQueryPlan) queryPlan;
		Assert.assertTrue(cqp.getTableTransformation().isEmpty());
	}
	
	@Test(timeout=60_000)
	public void testTablePlan2() {
		final ContinuousQueryPlan queryPlan = QueryPlanBuilder
			.createQueryOnTable("table")
			.forAllNewTuplesInSpace(new Hyperrectangle(3d, 4d))
			
			// Stream
			.filterStreamTupleByBoundingBox(new Hyperrectangle(3d, 5d))
			// Stored
			.filterStoredTupleByBoundingBox(new Hyperrectangle(3d, 6d))
			// Stream
			.enlargeStreamTupleBoundBoxByValue(4)
			// Stream
			.enlargeStreamTupleBoundBoxByFactor(2)
			// Stream
			.enlargeStreamTupleBoundBoxByWGS84Meter(7.2, 13.3)
			// Stored
			.filterStoredTupleByKey("abc")
			// Stream
			.filterStreamTupleByKey("def")
			.spatialJoinWithTable("testtable")
			.build();
		
		Assert.assertEquals("table", queryPlan.getStreamTable());
		Assert.assertEquals(new Hyperrectangle(3d, 4d), queryPlan.getQueryRange());
		Assert.assertEquals(5, queryPlan.getStreamTransformation().size());
		
		final ContinuousSpatialJoinQueryPlan cqp = (ContinuousSpatialJoinQueryPlan) queryPlan;
		System.out.println(cqp.getTableTransformation());
		Assert.assertEquals(2, cqp.getTableTransformation().size());
		Assert.assertEquals(5, cqp.getStreamTransformation().size());

	}
}
