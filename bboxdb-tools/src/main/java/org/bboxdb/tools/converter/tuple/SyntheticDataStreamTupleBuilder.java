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
package org.bboxdb.tools.converter.tuple;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyntheticDataStreamTupleBuilder extends TupleBuilder {
		
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SyntheticDataStreamTupleBuilder.class);

	@Override
	public Tuple buildTuple(final String valueData, final String keyData) {
		
		// Key, Box, Value
		// 2so3r7lmmlt2204 23.000566996237414,23.000566996237414,17.782247471059588,17.782247471059588,33.20003060813562,33.20003060813562,96.82571259235081,96.82571259235081 y9ucczzo53
		final String[] data = valueData.split(" ");
		
		if(data.length != 3) {
			throw new RuntimeException("Unable to decode tuple: " + valueData);
		}
		
		try {
			final String key = keyData != null ? keyData : data[0];

			final String[] bboxData = data[1].split(",");
			if(bboxData.length % 2 != 0) {
				logger.error("Invalid dimension for bbox data : " + data[1]);
				return null;
			}
	
			final double bboxValues[] = new double[bboxData.length];
			
			for(int i = 0; i < bboxData.length; i++) {
				bboxValues[i] = Double.parseDouble(bboxData[i]);
			}
			
			final Hyperrectangle boundingBox = new Hyperrectangle(bboxValues);
			
			final Tuple tuple = new Tuple(key, boundingBox, data[2].getBytes());
			
			return tuple;
		} catch (Exception e) {
			throw new RuntimeException("Unable to decode tuple (date parse): " + valueData, e);
		}
	}

}
