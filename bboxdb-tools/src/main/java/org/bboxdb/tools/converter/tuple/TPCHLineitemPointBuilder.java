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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TPCHLineitemPointBuilder implements TupleBuilder {

	/**
	 * The date parser
	 */
	protected final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd");
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TPCHLineitemPointBuilder.class);

	
	@Override
	/**
	 * Orderkey     - 0
	 * Partkey      - 1 
	 * Suppkey      - 2
	 * Linenumer    - 3
	 * Quantity     - 4
	 * Price        - 5
	 * Discount     - 6 
	 * Tax          - 7
	 * Returnflag   - 8
	 * Linestatus   - 9
	 * Shippdate    - 10
	 * Commitdate   - 11
	 * Receiptdate  - 12
	 * Shipinstruct - 13
	 * Shipmode     - 14
	 * Comment      - 15
	 */
	// 3|29380|1883|4|2|2618.76|0.01|0.06|A|F|1993-12-04|1994-01-07|1994-01-01|NONE|TRUCK|y. fluffily pending d|
	public Tuple buildTuple(final String keyData, final String valueData) {
		final String[] data = valueData.split("\\|");	
	
		try {
			final Date shipDate = dateParser.parse(data[10]);
			
			final double shipDateTime = (double) shipDate.getTime();
			final BoundingBox boundingBox = new BoundingBox(shipDateTime, shipDateTime);
			
			final Tuple tuple = new Tuple(keyData, boundingBox, valueData.getBytes());
			
			return tuple;
		} catch (ParseException e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}

}
