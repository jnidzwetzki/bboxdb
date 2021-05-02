/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
import org.bboxdb.network.query.filter.UserDefinedGeoJsonSpatialFilter;
import org.bboxdb.storage.entity.Tuple;
import org.junit.Assert;
import org.junit.Test;

public class TestGeoJSONUDF {
	
	public final static String POINT_1 = "{\"geometry\":{\"coordinates\":[13.428588000000001,52.5481544],\"type\":\"Point\"},\"id\":378205943,\"type\":\"Feature\"}";

	public final static Tuple POINT_TUPLE_1 = new Tuple("1", Hyperrectangle.FULL_SPACE, POINT_1.getBytes());
	
	public final static String POINT_2 = "{\"geometry\":{\"coordinates\":[13.428776500000001,52.547766900000006],\"type\":\"Point\"},\"id\":378205943,\"type\":\"Feature\"}";

	public final static Tuple POINT_TUPLE_2 = new Tuple("2", Hyperrectangle.FULL_SPACE, POINT_2.getBytes());
	
	public final static String POINT_3 = "{\"geometry\":{\"coordinates\":[13.428776500000101,52.547766900003006],\"type\":\"Point\"},\"id\":378205943,\"type\":\"Feature\"}";

	public final static Tuple POINT_TUPLE_3 = new Tuple("3", Hyperrectangle.FULL_SPACE, POINT_3.getBytes());
	
	
	public final static String LINE_STRING_1 = "{\"geometry\":{\"coordinates\":[[13.428776500000001,52.547766900000006],[13.428913000000001,52.54771890000001],[13.428980200000002,52.547695700000006],"
			+ "[13.4293648,52.547562500000005],[13.4295822,52.547487200000006],[13.430496000000002,52.547172800000006],[13.430548100000001,52.5471535],[13.431191100000001,52.546914],[13.4321686,52.5465911],"
			+ "[13.4331232,52.546266300000006],[13.434201000000002,52.5458914]],\"type\":\"LineString\"},\"id\":4615321,"
			+ "\"type\":\"Feature\",\"properties\":{\"surface\":\"asphalt\",\"maxspeed\":\"30\",\"name\":\"Erich-Weinert-Straße\",\"highway\":\"residential\",\"postal_code\":\"10409\"}}";
			
	public final static Tuple LINE_STRING_TUPLE_1 = new Tuple("2", Hyperrectangle.FULL_SPACE, LINE_STRING_1.getBytes());
	
	@Test(timeout = 60_000)
	public void testIntersect1() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter.filterJoinCandidate(POINT_TUPLE_1, POINT_TUPLE_1, "".getBytes()));
		Assert.assertTrue(filter.filterJoinCandidate(POINT_TUPLE_2, POINT_TUPLE_2, "".getBytes()));
		Assert.assertTrue(filter.filterJoinCandidate(POINT_TUPLE_3, POINT_TUPLE_3, "".getBytes()));
		Assert.assertTrue(filter.filterJoinCandidate(LINE_STRING_TUPLE_1, LINE_STRING_TUPLE_1, "".getBytes()));

		Assert.assertFalse(filter.filterJoinCandidate(POINT_TUPLE_1, LINE_STRING_TUPLE_1, "".getBytes()));
		Assert.assertFalse(filter.filterJoinCandidate(LINE_STRING_TUPLE_1, POINT_TUPLE_1, "".getBytes()));
		
		Assert.assertTrue(filter.filterJoinCandidate(POINT_TUPLE_2, LINE_STRING_TUPLE_1, "".getBytes()));
		Assert.assertTrue(filter.filterJoinCandidate(LINE_STRING_TUPLE_1, POINT_TUPLE_2, "".getBytes()));
		
		Assert.assertTrue(filter.filterJoinCandidate(POINT_TUPLE_3, LINE_STRING_TUPLE_1, "".getBytes()));
		Assert.assertTrue(filter.filterJoinCandidate(LINE_STRING_TUPLE_1, POINT_TUPLE_3, "".getBytes()));
	}
}
