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
package org.bboxdb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.TupleFileReader;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;
import org.junit.Assert;
import org.junit.Test;


public class TestTupleBuilder {

	/**
	 * The testline for TPC-H lineitem tests
	 */
	private final static String TPCH_LINEITEM_TEST_LINE = "3|29380|1883|4|2|2618.76|0.01|0.06|A|F|1993-12-04|1994-01-07|1994-01-01|NONE|TRUCK|y. fluffily pending d|";

	/**
	 * The testline for TPC-H order tests
	 */
	private final static String TPCH_ORDER_TEST_LINE = "1|738001|O|215050.73|1996-01-02|5-LOW|Clerk#000019011|0|nstructions sleep furiously among |";

	/**
	 * The testline for synthetic tests
	 */
	private final static String SYNTHETIC_TEST_LINE = "51.47015078569419,58.26664175357267,49.11808592466023,52.72529828070016 e1k141dox9rayxo544y9";

	/**
	 * The testline for yellow taxi format tests
	 */
	private final static String TAXI_TEST_LINE = "2,2016-01-01 00:00:00,2016-01-01 00:00:00,2,1.10,-73.990371704101563,40.734695434570313,1,N,-73.981842041015625,40.732406616210937,2,7.5,0.5,0.5,0,0,0.3,8.8";

	/**
	 * The test lines for the rome taxi data set
	 */
	private final static String ROME_TAXI_1 = "173;2014-01-04 00:00:07.028304+01;POINT(41.91924450823211 12.5027184734508)";
	private final static String ROME_TAXI_2 = "173;2014-01-04 00:00:09.028304+01;POINT(41.92924450823211 14.5527184734508)";

	/**
	 * The line for the forex tests
	 */
	private final static String FOREX_LINE = "20151201 010000000,1.057520,1.057560,0";

	/**
	 * The line for geojson tests
	 */
	private final static String GEO_JSON_LINE = "{\"geometry\":{\"coordinates\":[52.4688608,13.3327994],\"type\":\"Point\"},\"id\":271247324,\"type\":\"Feature\",\"properties\":{\"natural\":\"tree\",\"leaf_cycle\":\"deciduous\",\"name\":\"Kaisereiche\",\"leaf_type\":\"broadleaved\",\"wikipedia\":\"de:Kaisereiche (Berlin)\"}}";

	/**
	 * The line for nari dynamic tests
	 */
	private final static String NARI_DYNAMIC = "245257000,0,0,0.1,13.1,36,-4.4657183,48.38249,1443650402";
	
	/**
	 * The line for BerlinMod player data
	 */
	private final static String BERLINMOD_PLAYER = "28-05-2007 06:02:16,272,14773,13.2983,52.5722";
	
	/**
	 * The line for BerlinMod data
	 */
	private final static String BERLINMOD = "2000,292882,2007-05-27,2007-05-28 09:00:34.446,13.327,52.4981,13.327,52.4981";
	
	/**
	 * The ADS-B Messages
	 */
	// Flight - AW119KX
	private final static String ADS_B_1 = "MSG,1,0,0,71B4,0,2020/01/30,16:36:35.000,2020/01/30,16:36:34.000,AW119KX,,,,,,,,,,,";
	private final static String ADS_B_2 = "MSG,3,0,0,71B4,0,2020/01/30,16:36:35.000,2020/01/30,16:36:34.000,,6600,,,-34.003143,18.668631,,,,,,";
	private final static String ADS_B_3 = "MSG,4,0,0,71B4,0,2020/01/30,16:36:35.000,2020/01/30,16:36:34.000,,,125.706009,297.992065,,,-64,,,,,";
	
	// Flight - MSR706
	private final static String ADS_B_4 = "MSG,1,0,0,1010B,0,2020/01/30,16:36:35.000,2020/01/30,16:36:34.000,MSR706,,,,,,,,,,,";
	private final static String ADS_B_5 = "MSG,3,0,0,1010B,0,2020/01/30,16:36:35.000,2020/01/30,16:36:34.000,,37000,,,45.088634,12.469348,,,,,,";
	private final static String ADS_B_6 = "MSG,4,0,0,1010B,0,2020/01/30,16:36:35.000,2020/01/30,16:36:34.000,,,487.345886,102.924118,,,0,,,,,";
	
	// Flight - AW119KX
	private final static String ADS_B_7 = "MSG,1,0,0,71B4,0,2020/01/30,16:36:40.000,2020/01/30,16:36:39.000,AW119KX,,,,,,,,,,,";
	private final static String ADS_B_8 = "MSG,3,0,0,71B4,0,2020/01/30,16:36:40.000,2020/01/30,16:36:39.000,,6500,,,-34.001831,18.665827,,,,,,";
	private final static String ADS_B_9 = "MSG,4,0,0,71B4,0,2020/01/30,16:36:40.000,2020/01/30,16:36:39.000,,,126.909416,300.808899,,,-128,,,,,";
	
	// Flight - MSR706
	private final static String ADS_B_10 = "MSG,1,0,0,1010B,0,2020/01/30,16:37:01.000,2020/01/30,16:37:00.000,MSR706,,,,,,,,,,,";
	private final static String ADS_B_11 = "MSG,3,0,0,1010B,0,2020/01/30,16:37:01.000,2020/01/30,16:37:00.000,,37010,,,45.076355,12.545340,,,,,,";
	private final static String ADS_B_12 = "MSG,4,0,0,1010B,0,2020/01/30,16:37:01.000,2020/01/30,16:37:00.000,,,488.320587,102.897881,,,0,,,,,";

	/**
	 * Test ADS-B tuple builder (2d)
	 */
	@Test
	public void testADSBTupleBuilder1() {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.ADSB_2D);
		
		final Tuple tuple1 = tupleBuilder.buildTuple(ADS_B_1);
		final Tuple tuple2 = tupleBuilder.buildTuple(ADS_B_2);
		final Tuple tuple3 = tupleBuilder.buildTuple(ADS_B_3);
		final Tuple tuple4 = tupleBuilder.buildTuple(ADS_B_4);
		final Tuple tuple5 = tupleBuilder.buildTuple(ADS_B_5);
		final Tuple tuple6 = tupleBuilder.buildTuple(ADS_B_6);
		final Tuple tuple7 = tupleBuilder.buildTuple(ADS_B_7);
		final Tuple tuple8 = tupleBuilder.buildTuple(ADS_B_8);
		final Tuple tuple9 = tupleBuilder.buildTuple(ADS_B_9);
		final Tuple tuple10 = tupleBuilder.buildTuple(ADS_B_10);
		final Tuple tuple11 = tupleBuilder.buildTuple(ADS_B_11);
		final Tuple tuple12 = tupleBuilder.buildTuple(ADS_B_12);
		
		Assert.assertNull(tuple1);
		Assert.assertNull(tuple2);
		Assert.assertNotNull(tuple3);
		Assert.assertNull(tuple4);
		Assert.assertNull(tuple5);
		Assert.assertNotNull(tuple6);
		Assert.assertNull(tuple7);
		Assert.assertNull(tuple8);
		Assert.assertNotNull(tuple9);
		Assert.assertNull(tuple10);
		Assert.assertNull(tuple11);
		Assert.assertNotNull(tuple12);
		
		final GeoJsonPolygon geoJson1 = GeoJsonPolygon.fromGeoJson(new String(tuple3.getDataBytes()));
		final GeoJsonPolygon geoJson2 = GeoJsonPolygon.fromGeoJson(new String(tuple6.getDataBytes()));
		final GeoJsonPolygon geoJson3 = GeoJsonPolygon.fromGeoJson(new String(tuple9.getDataBytes()));
		final GeoJsonPolygon geoJson4 = GeoJsonPolygon.fromGeoJson(new String(tuple12.getDataBytes()));
			
		Assert.assertEquals("AW119KX", geoJson1.getProperties().get("Callsign"));
		Assert.assertEquals("6600", geoJson1.getProperties().get("Altitude"));
		Assert.assertEquals("125.706009", geoJson1.getProperties().get("GroundSpeed"));
		Assert.assertEquals("297.992065", geoJson1.getProperties().get("Track"));
		Assert.assertEquals(2, tuple3.getBoundingBox().getDimension());
		Assert.assertEquals(18.668631, tuple3.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(-34.003143, tuple3.getBoundingBox().getCoordinateHigh(0), 0.00001);

		Assert.assertEquals("MSR706", geoJson2.getProperties().get("Callsign"));
		Assert.assertEquals("37000", geoJson2.getProperties().get("Altitude"));
		Assert.assertEquals("487.345886", geoJson2.getProperties().get("GroundSpeed"));
		Assert.assertEquals("102.924118", geoJson2.getProperties().get("Track"));
		Assert.assertEquals(2, tuple6.getBoundingBox().getDimension());
		Assert.assertEquals(12.469348, tuple6.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(45.088634, tuple6.getBoundingBox().getCoordinateHigh(0), 0.00001);

		Assert.assertEquals("AW119KX", geoJson3.getProperties().get("Callsign"));
		Assert.assertEquals("6500", geoJson3.getProperties().get("Altitude"));
		Assert.assertEquals("126.909416", geoJson3.getProperties().get("GroundSpeed"));
		Assert.assertEquals("300.808899", geoJson3.getProperties().get("Track"));
		Assert.assertEquals(2, tuple9.getBoundingBox().getDimension());
		Assert.assertEquals(18.665827, tuple9.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(-34.001831, tuple9.getBoundingBox().getCoordinateHigh(0), 0.00001);

		Assert.assertEquals("MSR706", geoJson4.getProperties().get("Callsign"));
		Assert.assertEquals("37010", geoJson4.getProperties().get("Altitude"));
		Assert.assertEquals("488.320587", geoJson4.getProperties().get("GroundSpeed"));
		Assert.assertEquals("102.897881", geoJson4.getProperties().get("Track"));
		Assert.assertEquals(2, tuple12.getBoundingBox().getDimension());
		Assert.assertEquals(12.54534, tuple12.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(45.076355, tuple12.getBoundingBox().getCoordinateHigh(0), 0.00001);	
	}
	
	/**
	 * Test ADS-B tuple builder - wrong order
	 */
	@Test
	public void testADSBTupleBuilder2() {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.ADSB_2D);
		
		final Tuple tuple1 = tupleBuilder.buildTuple(ADS_B_3);
		final Tuple tuple2 = tupleBuilder.buildTuple(ADS_B_2);
		final Tuple tuple3 = tupleBuilder.buildTuple(ADS_B_1);
		final Tuple tuple4 = tupleBuilder.buildTuple(ADS_B_3);

		Assert.assertNull(tuple1);
		Assert.assertNull(tuple2);
		Assert.assertNull(tuple3);
		Assert.assertNotNull(tuple4);
	}
	
	/**
	 * Test ADS-B tuple builder
	 */
	@Test
	public void testADSBTupleBuilder3() {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.ADSB_3D);
		
		final Tuple tuple1 = tupleBuilder.buildTuple(ADS_B_1);
		final Tuple tuple2 = tupleBuilder.buildTuple(ADS_B_2);
		final Tuple tuple3 = tupleBuilder.buildTuple(ADS_B_3);
		final Tuple tuple4 = tupleBuilder.buildTuple(ADS_B_4);
		final Tuple tuple5 = tupleBuilder.buildTuple(ADS_B_5);
		final Tuple tuple6 = tupleBuilder.buildTuple(ADS_B_6);
		final Tuple tuple7 = tupleBuilder.buildTuple(ADS_B_7);
		final Tuple tuple8 = tupleBuilder.buildTuple(ADS_B_8);
		final Tuple tuple9 = tupleBuilder.buildTuple(ADS_B_9);
		final Tuple tuple10 = tupleBuilder.buildTuple(ADS_B_10);
		final Tuple tuple11 = tupleBuilder.buildTuple(ADS_B_11);
		final Tuple tuple12 = tupleBuilder.buildTuple(ADS_B_12);
		
		Assert.assertNull(tuple1);
		Assert.assertNull(tuple2);
		Assert.assertNotNull(tuple3);
		Assert.assertNull(tuple4);
		Assert.assertNull(tuple5);
		Assert.assertNotNull(tuple6);
		Assert.assertNull(tuple7);
		Assert.assertNull(tuple8);
		Assert.assertNotNull(tuple9);
		Assert.assertNull(tuple10);
		Assert.assertNull(tuple11);
		Assert.assertNotNull(tuple12);
			
		Assert.assertEquals(3, tuple3.getBoundingBox().getDimension());
		Assert.assertEquals(18.668631, tuple3.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(-34.003143, tuple3.getBoundingBox().getCoordinateHigh(0), 0.00001);
		Assert.assertEquals(6600, tuple3.getBoundingBox().getCoordinateHigh(2), 0.00001);

		Assert.assertEquals(3, tuple6.getBoundingBox().getDimension());
		Assert.assertEquals(12.469348, tuple6.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(45.088634, tuple6.getBoundingBox().getCoordinateHigh(0), 0.00001);
		Assert.assertEquals(37000, tuple6.getBoundingBox().getCoordinateHigh(2), 0.00001);

		Assert.assertEquals(3, tuple9.getBoundingBox().getDimension());
		Assert.assertEquals(18.665827, tuple9.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(-34.001831, tuple9.getBoundingBox().getCoordinateHigh(0), 0.00001);
		Assert.assertEquals(6500, tuple9.getBoundingBox().getCoordinateHigh(2), 0.00001);

		Assert.assertEquals(3, tuple12.getBoundingBox().getDimension());
		Assert.assertEquals(12.54534, tuple12.getBoundingBox().getCoordinateHigh(1), 0.00001);
		Assert.assertEquals(45.076355, tuple12.getBoundingBox().getCoordinateHigh(0), 0.00001);	
		Assert.assertEquals(37010, tuple12.getBoundingBox().getCoordinateHigh(2), 0.00001);
	}

	/**
	 * Test the geo json tuple builder
	 */
	@Test
	public void testGeoJsonTupleBuilder() {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.GEOJSON);

		final Tuple tuple = tupleBuilder.buildTuple(GEO_JSON_LINE, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());
		final Hyperrectangle expectedBox = new Hyperrectangle(13.3327994, 13.3327994, 52.4688608, 52.4688608);
		Assert.assertEquals(expectedBox, tuple.getBoundingBox());
	}

	/**
	 * Test the rome taxi range tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testRomeTaxiPointTupleBuilder() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.ROME_TAXI_POINT);

		final Tuple tuple = tupleBuilder.buildTuple(ROME_TAXI_1, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(1388790007028d, 1388790007028d,
				41.91924450823211, 41.91924450823211,
				12.5027184734508, 12.5027184734508);

		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}
	
	/**
	 * Test the forex 1d tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testForex1DTupleBuilder() throws ParseException {
		
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.FOREX_1D);

		final Tuple tuple1 = tupleBuilder.buildTuple(FOREX_LINE, "1");

		Assert.assertNotNull(tuple1);
		Assert.assertEquals(Integer.toString(1), tuple1.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(1.05752d, 1.05752d);

		Assert.assertEquals(exptectedBox, tuple1.getBoundingBox());
	}
	
	/**
	 * Test the forex 2d tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testForex2DTupleBuilder() throws ParseException {
		
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.FOREX_2D);

		final Tuple tuple1 = tupleBuilder.buildTuple(FOREX_LINE, "1");

		Assert.assertNotNull(tuple1);
		Assert.assertEquals(Integer.toString(1), tuple1.getKey());
		
		Assert.assertEquals(2, tuple1.getBoundingBox().getDimension());
		Assert.assertEquals(tuple1.getBoundingBox().getCoordinateHigh(1), 1.05752d, 0.1);
	}


	/**
	 * Test the rome taxi range tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testRomeTaxiRangeTupleBuilder1() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.ROME_TAXI_RANGE);

		final Tuple tuple1 = tupleBuilder.buildTuple(ROME_TAXI_1, "1");
		final Tuple tuple2 = tupleBuilder.buildTuple(ROME_TAXI_2, "1");

		Assert.assertNull(tuple1);
		Assert.assertNotNull(tuple2);
		Assert.assertEquals(Integer.toString(1), tuple2.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(1388790007028d, 1388790009028d,
				41.91924450823211, 41.92924450823211,
				12.5027184734508, 14.5527184734508);

		Assert.assertEquals(exptectedBox, tuple2.getBoundingBox());
	}

	/**
	 * Test the rome taxi range tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testRomeTaxiRangeTupleBuilder2() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.ROME_TAXI_RANGE);

		final Tuple tuple1 = tupleBuilder.buildTuple(ROME_TAXI_2, "1");
		final Tuple tuple2 = tupleBuilder.buildTuple(ROME_TAXI_1, "1");

		Assert.assertNull(tuple1);
		Assert.assertNotNull(tuple2);
		Assert.assertEquals(Integer.toString(1), tuple2.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(1388790007028d, 1388790009028d,
				41.91924450823211, 41.92924450823211,
				12.5027184734508, 14.5527184734508);

		Assert.assertEquals(exptectedBox, tuple2.getBoundingBox());
	}

	/**
	 * Test the rome taxi range tuple builder - with padding
	 * @throws ParseException
	 */
	@Test
	public void testRomeTaxiRangeTupleBuilder3() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.ROME_TAXI_RANGE);

		tupleBuilder.setPadding(1.0);

		final Tuple tuple1 = tupleBuilder.buildTuple(ROME_TAXI_2, "1");
		final Tuple tuple2 = tupleBuilder.buildTuple(ROME_TAXI_1, "1");

		Assert.assertNull(tuple1);
		Assert.assertNotNull(tuple2);
		Assert.assertEquals(Integer.toString(1), tuple2.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(1388790007027d, 1388790009029d,
				40.91924450823211, 42.92924450823211,
				11.5027184734508, 15.5527184734508);

		Assert.assertEquals(exptectedBox, tuple2.getBoundingBox());
	}

	/**
	 * Test the yellow taxi range tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testYellowTaxiRangeTupleBuilder() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.YELLOWTAXI_RANGE);

		final Tuple tuple = tupleBuilder.buildTuple(TAXI_TEST_LINE, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
		final Date dateLow = dateParser.parse("2016-01-01 00:00:00");
		final Date dateHigh = dateParser.parse("2016-01-01 00:00:00");

		final Hyperrectangle exptectedBox = new Hyperrectangle(-73.990371704101563, -73.981842041015625,
				40.732406616210937, 40.734695434570313,
				(double) dateLow.getTime(), (double) dateHigh.getTime());

		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}

	/**
	 * Test the yellow taxi range tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testYellowTaxiPointTupleBuilder() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.YELLOWTAXI_POINT);

		final Tuple tuple = tupleBuilder.buildTuple(TAXI_TEST_LINE, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");
		final Date dateLow = dateParser.parse("2016-01-01 00:00:00");

		final Hyperrectangle exptectedBox = new Hyperrectangle(-73.990371704101563, -73.990371704101563,
				40.734695434570313, 40.734695434570313,
				(double) dateLow.getTime(), (double) dateLow.getTime());

		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}

	/**
	 * Test the tpch point tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testTPCHLineitemPointTupleBuilder() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.TPCH_LINEITEM_POINT);

		final Tuple tuple = tupleBuilder.buildTuple(TPCH_LINEITEM_TEST_LINE, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd");
		final Date date = dateParser.parse("1993-12-04");

		final double doubleTime = (double) date.getTime();
		final Hyperrectangle exptectedBox = new Hyperrectangle(doubleTime, doubleTime);

		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}

	/**
	 * Test the tpch range tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testTPCHLineitemRangeTupleBuilder() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.TPCH_LINEITEM_RANGE);

		final Tuple tuple = tupleBuilder.buildTuple(TPCH_LINEITEM_TEST_LINE, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd");
		final Date shipDateTime = dateParser.parse("1993-12-04");
		final Date receiptDateTime = dateParser.parse("1994-01-01");

		final double doubleShipDateTime = (double) shipDateTime.getTime();
		final double doublereceiptDateTime = (double) receiptDateTime.getTime();

		final Hyperrectangle exptectedBox = new Hyperrectangle(doubleShipDateTime, doublereceiptDateTime);

		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}

	/**
	 * Test the tpch range tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testTPCHOrderPointTupleBuilder() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.TPCH_ORDER_POINT);

		final Tuple tuple = tupleBuilder.buildTuple(TPCH_ORDER_TEST_LINE, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd");
		final Date orderDate = dateParser.parse("1996-01-02");

		final double doubleOrder = (double) orderDate.getTime();

		final Hyperrectangle expectedBox = new Hyperrectangle(doubleOrder, doubleOrder);

		Assert.assertEquals(expectedBox, tuple.getBoundingBox());
	}

	/**
	 * Test the syntetic tuple builder
	 * @throws ParseException
	 */
	@Test
	public void testSyntheticTupleBuilder() throws ParseException {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.SYNTHETIC);

		final Tuple tuple = tupleBuilder.buildTuple(SYNTHETIC_TEST_LINE, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(51.47015078569419, 58.26664175357267,
				49.11808592466023, 52.72529828070016);

		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
		Assert.assertEquals("e1k141dox9rayxo544y9", new String(tuple.getDataBytes()));
	}

	/**
	 * Test the nari dynamic tuple builder
	 */
	@Test
	public void testNariDynamicTupleBuilder() {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.NARI_DYNAMIC);

		final Tuple tuple = tupleBuilder.buildTuple(NARI_DYNAMIC, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(1443650402d, 1443650402d,
				-4.4657183d, -4.4657183d, 48.38249d, 48.38249d);
		
		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}
	
	/**
	 * Test the berlinmod player tuple builder
	 */
	@Test
	public void testBerlinModPlayerTupleBuilder() {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.BERLINMOD_PLAYER);

		final Tuple tuple = tupleBuilder.buildTuple(BERLINMOD_PLAYER, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(52.5722d, 52.5722d, 13.2983d, 13.2983d);
		
		Assert.assertEquals(1180332136000L, tuple.getVersionTimestamp());
		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}
	
	/**
	 * Test the berlinmod tuple builder
	 */
	@Test
	public void testBerlinModTupleBuilder() {
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.BERLINMOD);

		final Tuple tuple = tupleBuilder.buildTuple(BERLINMOD, "1");

		Assert.assertNotNull(tuple);
		Assert.assertEquals(Integer.toString(1), tuple.getKey());

		final Hyperrectangle exptectedBox = new Hyperrectangle(52.4981d, 52.4981d, 13.327d, 13.327d);
				
		Assert.assertEquals(1180224000000L, tuple.getVersionTimestamp());
		Assert.assertEquals(exptectedBox, tuple.getBoundingBox());
	}

	/**
	 * Test the tuple file builder - Process non existing file
	 * @throws IOException
	 */
	@Test(expected=IOException.class)
	public void testTupleFile1() throws Exception {
		final File tempFile = File.createTempFile("temp",".txt");

		tempFile.delete();
		Assert.assertFalse(tempFile.exists());

		final TupleFileReader tupleFile = new TupleFileReader(tempFile.getAbsolutePath(),
				TupleBuilderFactory.Name.GEOJSON);

		tupleFile.processFile();
	}

	/**
	 * Test the tuple file builder
	 * @throws IOException
	 */
	@Test
	public void testTupleFile2() throws Exception {
		final File tempFile = File.createTempFile("temp",".txt");
		tempFile.deleteOnExit();

		// The reference tuple
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.GEOJSON);
		final Tuple tuple = tupleBuilder.buildTuple(GEO_JSON_LINE, "1");

		final BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
		writer.write(GEO_JSON_LINE);
		writer.write("\n");
		writer.close();

		final TupleFileReader tupleFile = new TupleFileReader(tempFile.getAbsolutePath(),
				TupleBuilderFactory.Name.GEOJSON);

		final AtomicInteger seenTuples = new AtomicInteger(0);

		tupleFile.addTupleListener(t -> {
			Assert.assertEquals(tuple.getKey(), t.getKey());
			Assert.assertEquals(tuple.getBoundingBox(), t.getBoundingBox());
			Assert.assertArrayEquals(tuple.getDataBytes(), t.getDataBytes());
			seenTuples.incrementAndGet();
		});

		tupleFile.processFile();

		Assert.assertEquals(1, seenTuples.get());
	}

	/**
	 * Test the tuple file builder
	 * @throws IOException
	 */
	@Test
	public void testTupleFile3() throws Exception {
		final File tempFile = File.createTempFile("temp",".txt");
		tempFile.deleteOnExit();

		// The reference tuple
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.GEOJSON);
		final Tuple tuple = tupleBuilder.buildTuple(GEO_JSON_LINE, "1");

		final BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
		writer.write(GEO_JSON_LINE);
		writer.write("\n");
		writer.write(GEO_JSON_LINE);
		writer.write("\n");
		writer.close();

		final TupleFileReader tupleFile = new TupleFileReader(tempFile.getAbsolutePath(),
				TupleBuilderFactory.Name.GEOJSON);

		final AtomicInteger seenTuples = new AtomicInteger(0);

		tupleFile.addTupleListener(t -> {
			Assert.assertEquals(tuple.getKey(), t.getKey());
			Assert.assertEquals(tuple.getBoundingBox(), t.getBoundingBox());
			Assert.assertArrayEquals(tuple.getDataBytes(), t.getDataBytes());
			seenTuples.incrementAndGet();
		});

		tupleFile.processFile(1);

		Assert.assertEquals(1, seenTuples.get());
		Assert.assertEquals(2, tupleFile.getProcessedLines());
		Assert.assertEquals(GEO_JSON_LINE, tupleFile.getLastReadLine());
	}

	/**
	 * Test the tuple file builder - Read 5 of 3 lines
	 * @throws IOException
	 */
	@Test
	public void testTupleFile4() throws Exception {
		final File tempFile = File.createTempFile("temp",".txt");
		tempFile.deleteOnExit();

		// The reference tuple
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(
				TupleBuilderFactory.Name.GEOJSON);
		final Tuple tuple = tupleBuilder.buildTuple(GEO_JSON_LINE, "1");

		final BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));
		writer.write(GEO_JSON_LINE);
		writer.write("\n");
		writer.write(GEO_JSON_LINE);
		writer.write("\n");
		writer.write(GEO_JSON_LINE);
		writer.write("\n");
		writer.close();

		final TupleFileReader tupleFile = new TupleFileReader(tempFile.getAbsolutePath(),
				TupleBuilderFactory.Name.GEOJSON);

		final AtomicInteger seenTuples = new AtomicInteger(0);

		tupleFile.addTupleListener(t -> {
			Assert.assertEquals(tuple.getBoundingBox(), t.getBoundingBox());
			Assert.assertArrayEquals(tuple.getDataBytes(), t.getDataBytes());
			seenTuples.incrementAndGet();
		});

		tupleFile.processFile(3);

		Assert.assertEquals(3, seenTuples.get());
	}
}
