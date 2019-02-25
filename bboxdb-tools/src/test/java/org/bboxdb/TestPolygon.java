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
package org.bboxdb;

import org.bboxdb.tools.converter.osm.util.Polygon;
import org.junit.Assert;
import org.junit.Test;

public class TestPolygon {

	@Test
	public void testDoubleAdd() {
		final Polygon polygon = new Polygon(47);
		Assert.assertTrue(polygon.addPoint(4, 5));
		Assert.assertFalse(polygon.addPoint(4, 5));
		Assert.assertTrue(polygon.addPoint(67, 45));
		Assert.assertFalse(polygon.addPoint(67, 45));
		Assert.assertTrue(polygon.addPoint(4, 5));
		Assert.assertTrue(polygon.addPoint(67, 45));
	}
	
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
		final String json = polygon.toGeoJson(false);
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

		final String json = polygon.toGeoJson(false);
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

		final String json = polygon.toGeoJson(false);
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

		final String json = polygon.toGeoJson(false);
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

		final String json1 = polygon.toGeoJson(false);
		final String json2 = polygon.toFormatedGeoJson(false);
		
		final String json3 = polygon.toGeoJson();
		final String json4 = polygon.toFormatedGeoJson();

		final Polygon polygon2 = Polygon.fromGeoJson(json1);
		final Polygon polygon3 = Polygon.fromGeoJson(json2);
		final Polygon polygon4 = Polygon.fromGeoJson(json3);
		final Polygon polygon5 = Polygon.fromGeoJson(json4);
		
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertTrue(polygon.equals(polygon3));
		Assert.assertEquals(47, polygon2.getId());
		Assert.assertEquals(47, polygon3.getId());
		
		Assert.assertTrue(polygon4.equals(polygon5));
	}
	
	/**
	 * Test the automatically close of the polygons
	 */
	@Test
	public void testPolygonAutoClose() {
		final Polygon polygon = new Polygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);
		
		Assert.assertNotEquals(polygon.getPointList().get(0), 
				polygon.getPointList().get(polygon.getNumberOfPoints() - 1));
		
		final Polygon polygon2 = Polygon.fromGeoJson(polygon.toGeoJson(false));
		Assert.assertEquals(5, polygon2.getNumberOfPoints());
		Assert.assertNotEquals(polygon2.getPointList().get(0), 
				polygon2.getPointList().get(polygon2.getNumberOfPoints() - 1));
		
		final Polygon polygon3 = Polygon.fromGeoJson(polygon.toGeoJson());
		Assert.assertEquals(6, polygon3.getNumberOfPoints());
		Assert.assertEquals(polygon3.getPointList().get(0), 
				polygon3.getPointList().get(polygon3.getNumberOfPoints() - 1));
	}

	/**
	 * Test decoding OSM data
	 */
	@Test
	public void testOSMEncoding() {
		final String testLine = "{\"geometry\":{\"coordinates\":[[[52.512283800000006,13.4482379],[52.512195000000006,13.4483031],[52.512145200000006,13.4483396],[52.5118676,13.448543500000001],[52.5117856,13.448603700000001],[52.511732300000006,13.4486428],[52.5116651,13.4486921],[52.511389,13.4488949],[52.511228100000004,13.449013]]],\"type\":\"Polygon\"},\"id\":1,\"type\":\"Feature\",\"properties\":{\"surface\":\"asphalt\"}}";
		final Polygon polygon = Polygon.fromGeoJson(testLine);
		Assert.assertEquals(2, polygon.getBoundingBox().getDimension());
		Assert.assertTrue(polygon.getBoundingBox().getExtent(0) > 0);
		Assert.assertTrue(polygon.getBoundingBox().getExtent(1) > 0);
	}
}
