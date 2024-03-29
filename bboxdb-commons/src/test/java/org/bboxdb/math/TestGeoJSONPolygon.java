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
package org.bboxdb.math;

import java.awt.geom.Point2D;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.junit.Assert;
import org.junit.Test;

public class TestGeoJSONPolygon {

	@Test
	public void testDoubleAdd() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
		Assert.assertTrue(polygon.addPoint(4, 5));
		Assert.assertFalse(polygon.addPoint(4, 5));
		Assert.assertTrue(polygon.addPoint(67, 45));
		Assert.assertFalse(polygon.addPoint(67, 45));
		Assert.assertTrue(polygon.addPoint(4, 5));
		Assert.assertTrue(polygon.addPoint(67, 45));
	}
	
	@Test(expected=Exception.class)
	public void testJSONEncoding1() {
		// Na valid GeoJSON
		final GeoJsonPolygon polygon = new GeoJsonPolygon(45);
		final String json = polygon.toGeoJson();
		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(45, polygon2. getId());
	}

	@Test
	public void testJSONEncoding2() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
		polygon.addPoint(4, 5);
		final String json = polygon.toGeoJson();
		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(json);

		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}

	@Test
	public void testJSONEncoding3() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		final String json = polygon.toGeoJson();
		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}

	@Test
	public void testJSONEncoding4() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);

		final String json = polygon.toGeoJson();
		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}

	@Test
	public void testJSONEncoding5() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);

		polygon.addProperty("key1", "value1");

		final String json = polygon.toGeoJson();
		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}

	@Test
	public void testJSONEncoding6() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
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
		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(json);
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertEquals(47, polygon2. getId());
	}

	@Test
	public void testJSONEncoding7() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
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
		
		final String json3 = polygon.toGeoJson();
		final String json4 = polygon.toFormatedGeoJson();

		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(json1);
		final GeoJsonPolygon polygon3 = GeoJsonPolygon.fromGeoJson(json2);
		final GeoJsonPolygon polygon4 = GeoJsonPolygon.fromGeoJson(json3);
		final GeoJsonPolygon polygon5 = GeoJsonPolygon.fromGeoJson(json4);
		
		Assert.assertTrue(polygon.equals(polygon2));
		Assert.assertTrue(polygon.equals(polygon3));
		Assert.assertEquals(47, polygon2.getId());
		Assert.assertEquals(47, polygon3.getId());
		
		Assert.assertTrue(polygon4.equals(polygon5));
	}
	
	@Test
	public void testPolygonCoordinatesInvert() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);
		
		polygon.invertPolygonCoordinates();
		
		Assert.assertEquals(polygon.getPointList().get(0), new Point2D.Double(5, 4));
		Assert.assertEquals(polygon.getPointList().get(1), new Point2D.Double(45, 67));
		Assert.assertEquals(polygon.getPointList().get(2), new Point2D.Double(-4, 3));
		Assert.assertEquals(polygon.getPointList().get(3), new Point2D.Double(12, 0));
		Assert.assertEquals(polygon.getPointList().get(4), new Point2D.Double(4, 2));
	}
	
	/**
	 * Test the automatically close of the polygons
	 */
	@Test
	public void testPolygonType() {
		final GeoJsonPolygon polygon = new GeoJsonPolygon(47);
		polygon.addPoint(4, 5);
		polygon.addPoint(67, 45);
		polygon.addPoint(3, -4);
		polygon.addPoint(0, 12);
		polygon.addPoint(2, 4);
		
		Assert.assertNotEquals(polygon.getPointList().get(0), 
				polygon.getPointList().get(polygon.getNumberOfPoints() - 1));
		
		final String geoJson1 = polygon.toGeoJson();
		final GeoJsonPolygon polygon2 = GeoJsonPolygon.fromGeoJson(geoJson1);
		Assert.assertEquals(5, polygon2.getNumberOfPoints());
		Assert.assertTrue(geoJson1.contains("LineString"));
		
		// Add end point
		polygon.addPoint(4, 5);

		final String geoJson2 = polygon.toGeoJson();
		final GeoJsonPolygon polygon3 = GeoJsonPolygon.fromGeoJson(geoJson2);

		Assert.assertEquals(6, polygon3.getNumberOfPoints());
		Assert.assertTrue(geoJson2.contains("Polygon"));
	}

	/**
	 * Test decoding OSM data
	 */
	@Test
	public void testOSMEncoding() {
		final String testLine = "{\"geometry\":{\"coordinates\":[[[52.512283800000006,13.4482379],[52.512195000000006,13.4483031],[52.512145200000006,13.4483396],[52.5118676,13.448543500000001],[52.5117856,13.448603700000001],[52.511732300000006,13.4486428],[52.5116651,13.4486921],[52.511389,13.4488949],[52.511228100000004,13.449013]]],\"type\":\"Polygon\"},\"id\":1,\"type\":\"Feature\",\"properties\":{\"surface\":\"asphalt\"}}";
		final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(testLine);
		Assert.assertEquals(2, polygon.getBoundingBox().getDimension());
		Assert.assertTrue(polygon.getBoundingBox().getExtent(0) > 0);
		Assert.assertTrue(polygon.getBoundingBox().getExtent(1) > 0);
	}
	
	/**
	 * Test direct geometry
	 */
	@Test
	public void testDirectGeometyEncoding() {
		final String testLine = "{\"coordinates\":[[[52.512283800000006,13.4482379],[52.512195000000006,13.4483031],[52.512145200000006,13.4483396],[52.5118676,13.448543500000001],[52.5117856,13.448603700000001],[52.511732300000006,13.4486428],[52.5116651,13.4486921],[52.511389,13.4488949],[52.511228100000004,13.449013]]],\"type\":\"Polygon\"}}";
		final GeoJsonPolygon polygon = GeoJsonPolygon.fromGeoJson(testLine);
		Assert.assertEquals(2, polygon.getBoundingBox().getDimension());
		Assert.assertTrue(polygon.getBoundingBox().getExtent(0) > 0);
		Assert.assertTrue(polygon.getBoundingBox().getExtent(1) > 0);
		Assert.assertEquals(-1, polygon.getId());
	}
}
