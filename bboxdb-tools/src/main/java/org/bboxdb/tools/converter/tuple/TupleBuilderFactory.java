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
package org.bboxdb.tools.converter.tuple;

import java.util.Arrays;
import java.util.List;

public class TupleBuilderFactory {

	/**
	 * The known parser names
	 *
	 */
	public static class Name {
		/**
		 * The yellow taxi builder - 3d point version (begin long, begin lat, trip start)
		 * @see http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
		 */
		public static final String YELLOWTAXI_POINT = "yellowtaxi_point";
		
		/**
		 * The yellow taxi builder - 3d range version 
		 * (begin long, begin lat, trip start, end long, end lat, trip end)
		 * @see http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
		 */
		public static final String YELLOWTAXI_RANGE = "yellowtaxi_range";
		
		/**
		 * The GEOJson builder
		 */
		public static final String GEOJSON = "geojson";
		
		/**
		 * The synthetic builder
		 */
		public static final String SYNTHETIC = "synthetic";

		/**
		 * The TPC-H lineitem builder - point version (shipDateTime)
		 */
		public static final String TPCH_LINEITEM_POINT = "tpch_lineitem_point";
		
		/**
		 * The TPC-H lineitem builder - range version (shipDateTime - receiptDateTime)
		 */
		public static final String TPCH_LINEITEM_RANGE = "tpch_lineitem_range";
		
		/**
		 * The TPC-H order builder - point version (orderDate)
		 */
		public static final String TPCH_ORDER_POINT = "tpch_order_point";
	}
	
	/**
	 * All known builder
	 */
	public static final List<String> ALL_BUILDER = Arrays.asList(
			Name.GEOJSON, Name.SYNTHETIC,
			Name.YELLOWTAXI_POINT, Name.YELLOWTAXI_RANGE, 
			Name.TPCH_LINEITEM_POINT, Name.TPCH_LINEITEM_RANGE, 
			Name.TPCH_ORDER_POINT);

	/**
	 * Return the parser for the tuple format
	 * @param format
	 * @return
	 */
	public static TupleBuilder getBuilderForFormat(final String format) {
		if(Name.GEOJSON.equals(format)) {
			return new GeoJSONTupleBuilder();
		} else if(Name.SYNTHETIC.equals(format)) {
			return new SyntheticTupleBuilder();
		} else if(Name.YELLOWTAXI_POINT.equals(format)) {
			return new YellowTaxiPointTupleBuilder();
		} else if(Name.YELLOWTAXI_RANGE.equals(format)) {
			return new YellowTaxiRangeTupleBuilder();
		} else if(Name.TPCH_LINEITEM_POINT.equals(format)) {
			return new TPCHLineitemPointBuilder();
		} else if(Name.TPCH_LINEITEM_RANGE.equals(format)) {
			return new TPCHLineitemRangeBuilder();
		} else if(Name.TPCH_ORDER_POINT.equals(format)) {
			return new TPCHOrderPointBuilder();
		} else {
			throw new RuntimeException("Unknown format: " + format);
		}
	}

}
