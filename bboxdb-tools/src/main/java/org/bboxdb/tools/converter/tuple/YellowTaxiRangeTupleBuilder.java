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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YellowTaxiRangeTupleBuilder extends TupleBuilder {

	/**
	 * The date parser
	 */
	protected final SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-mm-dd HH:mm:ss");

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(YellowTaxiRangeTupleBuilder.class);

	@Override
	public Tuple buildTuple(final String keyData, final String valueData) {
		try {
			final String[] data = valueData.split(",");

			// Header of the file
			if("VendorID".equals(data[0])) {
				return null;
			}

			final Date tripStart = dateParser.parse(data[1]);
			final Date tripEnd = dateParser.parse(data[2]);

			final double longBegin = Double.parseDouble(data[5]);
			final double latBegin = Double.parseDouble(data[6]);

			final double longEnd = Double.parseDouble(data[9]);
			final double latEnd = Double.parseDouble(data[10]);

			final Hyperrectangle boundingBox = new Hyperrectangle(
					Math.min(longBegin, longEnd),
					Math.max(longBegin, longEnd),
					Math.min(latBegin, latEnd),
					Math.max(latBegin, latEnd),
					Math.min((double) tripStart.getTime(), (double) tripEnd.getTime()),
					Math.max((double) tripStart.getTime(), (double) tripEnd.getTime()));

			return new Tuple(keyData, boundingBox.enlargeByAmount(boxPadding), valueData.getBytes());
		} catch (NumberFormatException | ParseException e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}


}
