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

public class TPCHOrderPointBuilder implements TupleBuilder {

	/**
	 * The date parser
	 */
	protected final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd");
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TPCHOrderPointBuilder.class);

	
	@Override
	/**
	 * Orderkey       - 0
	 * Customer key   - 1
	 * Orderstatus    - 2 
	 * Totalprice     - 3
	 * Orderdate      - 4
	 * Priority       - 5
	 * Clerk          - 6
	 * Shippriority   - 7 
	 * Comment        - 8
	 */
	// 1|738001|O|215050.73|1996-01-02|5-LOW|Clerk#000019011|0|nstructions sleep furiously among |
	public Tuple buildTuple(final String keyData, final String valueData) {
		final String[] data = valueData.split("\\|");	
	
		try {
			final Date orderDate = dateParser.parse(data[4]);
			
			final double orderDateTime = (double) orderDate.getTime();
			final BoundingBox boundingBox = new BoundingBox(orderDateTime, orderDateTime);
			
			final Tuple tuple = new Tuple(keyData, boundingBox, valueData.getBytes());
			
			return tuple;
		} catch (ParseException e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}

}
