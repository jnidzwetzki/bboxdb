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
package org.bboxdb.test.tools;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.query.filter.UserDefinedGeoJsonSpatialFilter;
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
	
	final GeoJsonPolygon polygon1 = GeoJsonPolygon.fromGeoJson(GEO_JSON1);
	
	final Tuple tuple1 = new Tuple("1", polygon1.getBoundingBox(), GEO_JSON1.getBytes());
	
	final String GEO_JSON2 = "{\n" + 
			"    \"type\": \"Polygon\", \n" + 
			"    \"coordinates\": [\n" + 
			"        [[300, 100], [400, 400], [200, 400], [100, 200], [300, 100]]\n" + 
			"    ]\n" + 
			"}";
	
	final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(GEO_JSON2);

	final Tuple tuple2 = new Tuple("2", polygon2.getBoundingBox(), GEO_JSON2.getBytes());

	
	final String GEO_JSON3 = "{\n" + 
			"    \"type\": \"Polygon\", \n" + 
			"    \"coordinates\": [\n" + 
			"        [[25, 10], [40, 40], [20, 40], [10, 20], [25, 10]]\n" + 
			"    ]\n" + 
			"}";
	
	final GeoJsonPolygon polygon3 = GeoJsonPolygon.fromGeoJson(GEO_JSON3);

	final Tuple tuple3 = new Tuple("3", polygon3.getBoundingBox(), GEO_JSON3.getBytes());
	
	final String GEO_JSON4 = "{\"geometry\":{\"coordinates\":"
			+ "[[[52.433929400000004,13.4176815],[52.4338558,13.4175515],"
			+ "[52.433778100000005,13.417588400000001],[52.433696700000006,13.4176991],"
			+ "[52.433620000000005,13.4177312],[52.433498500000006,13.4177038],"
			+ "[52.433477100000005,13.417780800000001],[52.4334786,13.417866400000001],"
			+ "[52.4335226,13.417992000000002],[52.4335449,13.418151600000002],"
			+ "[52.433542800000005,13.4182857],[52.433566400000004,13.4184006],"
			+ "[52.433641900000005,13.4183781],[52.43370650000001,13.4182859],"
			+ "[52.4337505,13.4181451],[52.4338193,13.418046100000002],"
			+ "[52.433905900000006,13.417984500000001],[52.433940400000004,13.417840700000001],"
			+ "[52.433929400000004,13.4176815]]],"
			+ "\"type\":\"Polygon\"},\"id\":51019677,\"type\":\"Feature\","
			+ "\"properties\":{\"natural\":\"wood\",\"name\":\"Liebesinsel\",\"place\":\"islet\"}}";
	
	final Tuple tuple4 = new Tuple("4", Hyperrectangle.FULL_SPACE, GEO_JSON4.getBytes());
	
	final String GEO_JSON5 = "{\"geometry\":{\"coordinates\":[[13.3529075,52.5096137],"
			+ "[13.3532205,52.509593800000005],[13.3535318,52.509594400000005],"
			+ "[13.3539159,52.5096154],[13.3562676,52.509819300000004],"
			+ "[13.3564819,52.50983840000001]],\"type\":\"LineString\"},"
			+ "\"id\":66514461,\"type\":\"Feature\",\"properties\":"
			+ "{\"sidewalk\":\"right\",\"ref\":\"L 1137\","
			+ "\"maxspeed:conditional\":\"30@(07:00-17:00)\","
			+ "\"surface\":\"asphalt\",\"lanes\":\"2\",\"maxspeed\":\"50\","
			+ "\"name\":\"Tiergartenstraße\",\"highway\":\"secondary\","
			+ "\"postal_code\":\"10785\",\"source:maxspeed\":\"sign\",\"cycleway\":\"lane\"}}";

	final Tuple tuple5 = new Tuple("5", Hyperrectangle.FULL_SPACE, GEO_JSON5.getBytes());

	@Test
	public void testGeometryJoin1() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter.filterJoinCandidate(tuple1, tuple1, "".getBytes()));
		Assert.assertTrue(filter.filterJoinCandidate(tuple2, tuple2, "".getBytes()));
		Assert.assertTrue(filter.filterJoinCandidate(tuple3, tuple3, "".getBytes()));
	}
	
	@Test
	public void testGeometryJoin2() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertFalse(filter.filterJoinCandidate(tuple1, tuple2, "".getBytes()));
		Assert.assertFalse(filter.filterJoinCandidate(tuple3, tuple2, "".getBytes()));
	}
	
	@Test
	public void testGeometryJoin3() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter.filterJoinCandidate(tuple1, tuple3, "".getBytes()));
	}
	
	@Test
	public void testGeometryJoin4() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertFalse(filter.filterJoinCandidate(tuple1, tuple4, "".getBytes()));
	}
	
	@Test
	public void testGeometryJoin5() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter.filterJoinCandidate(tuple4, tuple4, "".getBytes()));
	}
	
	@Test
	public void testGeometryJoin6() {
		final UserDefinedGeoJsonSpatialFilter filter = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter.filterJoinCandidate(tuple5, tuple5, "".getBytes()));
	}
	
	@Test
	public void testGeometryFilter1() {
		final UserDefinedGeoJsonSpatialFilter filter1 = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter1.filterTuple(tuple1, tuple1.getDataBytes()));
		
		final UserDefinedGeoJsonSpatialFilter filter2 = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter2.filterTuple(tuple2, tuple2.getDataBytes()));
		
		final UserDefinedGeoJsonSpatialFilter filter3 = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertTrue(filter3.filterTuple(tuple3, tuple3.getDataBytes()));
	}
	
	@Test
	public void testGeometryFilter2() {
		final UserDefinedGeoJsonSpatialFilter filter1 = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertFalse(filter1.filterTuple(tuple1, tuple2.getDataBytes()));
		
		final UserDefinedGeoJsonSpatialFilter filter2 = new UserDefinedGeoJsonSpatialFilter();
		Assert.assertFalse(filter2.filterTuple(tuple3, tuple2.getDataBytes()));
	}
	
}
