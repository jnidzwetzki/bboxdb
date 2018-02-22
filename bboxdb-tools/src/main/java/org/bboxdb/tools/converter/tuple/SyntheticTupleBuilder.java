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

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyntheticTupleBuilder implements TupleBuilder {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SyntheticTupleBuilder.class);

	@Override
	public Tuple buildTuple(final String keyData, final String valueData) {
		try {
			final String[] data = valueData.split(" ");
			
			if(data.length != 2) {
				logger.error("Unable to split line : " + valueData);
				return null;
			}
			
			final String[] bboxData = data[0].split(",");
			if(bboxData.length % 2 != 0) {
				logger.error("Invalid dimension for bbox data : " + data[0]);
				return null;
			}
	
			final double bboxValues[] = new double[bboxData.length];
			
			for(int i = 0; i < bboxData.length; i++) {
				bboxValues[i] = Double.parseDouble(bboxData[i]);
			}
			
			final BoundingBox boundingBox = new BoundingBox(bboxValues);
			
			final Tuple tuple = new Tuple(keyData, boundingBox, data[1].getBytes());
			
			return tuple;
		} catch (NumberFormatException e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}

}
