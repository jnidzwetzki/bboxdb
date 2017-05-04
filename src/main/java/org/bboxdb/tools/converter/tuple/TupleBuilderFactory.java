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
		 * The yellow taxi builder
		 * @see http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
		 */
		private static final String YELLOWTAXI = "yellowtaxi";
		
		/**
		 * The GEOJson parser
		 */
		private static final String GEOJSON = "geojson";
	}
	
	/**
	 * All known builder
	 */
	public static final List<String> ALL_BUILDER = Arrays.asList(Name.GEOJSON, Name.YELLOWTAXI);

	/**
	 * Return the parser for the tuple format
	 * @param format
	 * @return
	 */
	public static TupleBuilder getBuilderForFormat(final String format) {
		if(Name.GEOJSON.equals(format)) {
			return new GeoJSONTupleBuilder();
		} else if(Name.YELLOWTAXI.equals(format)) {
			return new YellowTaxiTupleBuilder();
		} else {
			throw new RuntimeException("Unknown format: " + format);
		}
	}

}
