/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.tools;

import org.bboxdb.tools.converter.osm.util.Polygon;
import org.junit.Assert;
import org.junit.Test;

public class TestPolygon {

	@Test
	public void testJSONEncoding1() {
		final Polygon polygon = new Polygon(45);
		final String json = polygon.toGeoJson();
		final Polygon polygon2 = Polygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(45, polygon2. getId());
	}
	
	@Test
	public void testJSONEncoding2() {
		final Polygon polygon = new Polygon(47);
		polygon.addPoint(4, 5);
		final String json = polygon.toGeoJson();
		final Polygon polygon2 = Polygon.fromGeoJson(json);
		
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}
	
	@Test
	public void testJSONEncoding3() {
		final Polygon polygon = new Polygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		final String json = polygon.toGeoJson();
		final Polygon polygon2 = Polygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}
	
	@Test
	public void testJSONEncoding4() {
		final Polygon polygon = new Polygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);
		
		final String json = polygon.toGeoJson();
		final Polygon polygon2 = Polygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}
	
	@Test
	public void testJSONEncoding5() {
		final Polygon polygon = new Polygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);
		
		polygon.addProperty("key1", "value1");
		
		final String json = polygon.toGeoJson();
		final Polygon polygon2 = Polygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}
	
	@Test
	public void testJSONEncoding6() {
		final Polygon polygon = new Polygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);
		
		polygon.addProperty("key1", "value1");
		polygon.addProperty("key2", "value2");
		polygon.addProperty("key3", "value3");
		polygon.addProperty("key4", "value4");
		polygon.addProperty("key5", "value5");
		polygon.addProperty("key6", "value6");

		final String json = polygon.toGeoJson();
		final Polygon polygon2 = Polygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}
	
	@Test
	public void testJSONEncoding7() {
		final Polygon polygon = new Polygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);
		
		polygon.addProperty("key1", "value1");
		polygon.addProperty("key2", "value2");
		polygon.addProperty("key3", "value3");
		polygon.addProperty("key4", "value4");
		polygon.addProperty("key5", "value5");
		polygon.addProperty("key6", "value6");

		final String json1 = polygon.toGeoJson();
		final String json2 = polygon.toFormatedGeoJson();

		final Polygon polygon2 = Polygon.fromGeoJson(json1);
		final Polygon polygon3 = Polygon.fromGeoJson(json2);

		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertTrue(polygon.equals(polygon3));
		Assert.assertEquals(47, polygon2.getId());
		Assert.assertEquals(47, polygon3.getId());
	}

}
