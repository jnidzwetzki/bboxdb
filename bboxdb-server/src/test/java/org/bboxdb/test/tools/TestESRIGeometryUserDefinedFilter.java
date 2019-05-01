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
package org.bboxdb.test.tools;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.filter.UserDefinedGeoJsonSpatialFilter;
import org.bboxdb.storage.entity.Tuple;
import org.junit.Assert;
import org.junit.Test;


public class TestESRIGeometryUserDefinedFilter {
	
	final String GEO_JSON1 = "{\n" + 
			"    \"type\": \"Polygon\", \n" + 
			"    \"coordinates\": [\n" + 
			"        [[30, 10], [40, 40], [20, 40], [10, 20], [30, 10]]\n" + 
			"    ]\n" + 
			"}";
	
	final Tuple tuple1 = new Tuple("1", Hyperrectangle.FULL_SPACE, GEO_JSON1.getBytes());
	
	final String GEO_JSON2 = "{\n" + 
			"    \"type\": \"Polygon\", \n" + 
			"    \"coordinates\": [\n" + 
			"        [[300, 100], [400, 400], [200, 400], [100, 200], [300, 100]]\n" + 
			"    ]\n" + 
			"}";
	
	final Tuple tuple2 = new Tuple("2", Hyperrectangle.FULL_SPACE, GEO_JSON2.getBytes());

	
	final String GEO_JSON3 = "{\n" + 
			"    \"type\": \"Polygon\", \n" + 
			"    \"coordinates\": [\n" + 
			"        [[25, 10], [40, 40], [20, 40], [10, 20], [25, 10]]\n" + 
			"    ]\n" + 
			"}";
	
	final Tuple tuple3 = new Tuple("3", Hyperrectangle.FULL_SPACE, GEO_JSON3.getBytes());


	@Test
	public void testGeometry1() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter.filterJoinCandidate(tuple1, tuple1, ""));
		Assert.assertTrue(filter.filterJoinCandidate(tuple2, tuple2, ""));
		Assert.assertTrue(filter.filterJoinCandidate(tuple3, tuple3, ""));
	}
	
	@Test
	public void testGeometry2() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertFalse(filter.filterJoinCandidate(tuple1, tuple2, ""));
		Assert.assertFalse(filter.filterJoinCandidate(tuple3, tuple2, ""));
	}
	
	@Test
	public void testGeometry3() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter.filterJoinCandidate(tuple1, tuple3, ""));
	}
}
