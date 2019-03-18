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

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NariDynamicBuilder extends TupleBuilder {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NariDynamicBuilder.class);

	// sourcemmsi,navigationalstatus,rateofturn,speedoverground,courseoverground,trueheading,lon,lat,t
	//
	// [0] sourcemmsi,
	// [1] navigationalstatus,
	// [2] rateofturn,
	// [3] speedoverground,
	// [4] courseoverground,
	// [5] trueheading,
	// [6] lon,
	// [7] lat,
	// [8] t

	public Tuple buildTuple(final String keyData, final String valueData) {
		final String[] data = valueData.split(",");
		
		if(data.length != 9) {
			throw new IllegalArgumentException("Unable to split input: " + valueData);
		}

		try {

			final double time = Double.parseDouble(data[8]);
			final double longitude = Double.parseDouble(data[6]);
			final double latitude = Double.parseDouble(data[7]);

			final Hyperrectangle boundingBox = new Hyperrectangle(time, time,
					longitude, longitude, latitude, latitude);

			return new Tuple(keyData, boundingBox.enlargeByAmount(boxPadding), valueData.getBytes());
		} catch (Exception e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}

}
