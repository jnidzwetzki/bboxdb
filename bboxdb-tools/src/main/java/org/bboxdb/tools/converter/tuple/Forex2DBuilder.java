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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Forex2DBuilder extends TupleBuilder {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(Forex2DBuilder.class);
	
	/**
	 * The date parser for  20151201 000005720
	 */
	private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HHmmss");
	
	@Override
	public Tuple buildTuple(final String keyData, final String valueData) {
		try {
			final String[] data = valueData.split(",");
			
			if(data.length != 4) {
				throw new IllegalArgumentException("Unable split: " + valueData);
			}
			
			// 20151201 000005720
			final String datetime = data[0];
			final Date parsedTime = dateFormat.parse(datetime);
			final long time = (parsedTime.getTime() / 1000);
			
			final Optional<Double> bid = MathUtil.tryParseDouble(data[1]);
			
			if(! bid.isPresent()) {
				throw new IllegalArgumentException("Unable to parse: " + data[1]);
			}
			
			final Hyperrectangle boundingBox = new Hyperrectangle(
					(double) time, (double) time,
					bid.get(), bid.get());
			
			return new Tuple(keyData, boundingBox.enlargeByAmount(boxPadding), valueData.getBytes());
		} catch (Exception e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}

}
