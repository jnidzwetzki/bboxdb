/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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

import java.util.Optional;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Forex1DBuilder extends TupleBuilder {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Forex1DBuilder.class);

	@Override
	public Tuple buildTuple(final String valueData, final String keyData) {
		
		try {
			final String key = keyData != null ? keyData : getKey(valueData);
			
			final String[] data = valueData.split(",");
			
			if(data.length != 4) {
				throw new IllegalArgumentException("Unable split: " + valueData);
			}
			
			final Optional<Double> bid = MathUtil.tryParseDouble(data[1]);
			
			if(! bid.isPresent()) {
				throw new IllegalArgumentException("Unable to parse: " + data[1]);
			}
			
			final Hyperrectangle boundingBox = new Hyperrectangle(bid.get(), bid.get());
			
			return new Tuple(key, boundingBox.enlargeByAmount(boxPadding), valueData.getBytes());
		} catch (Exception e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}

}
