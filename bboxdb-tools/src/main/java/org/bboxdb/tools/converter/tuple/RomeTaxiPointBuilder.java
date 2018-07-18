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

import java.time.LocalDateTime;
import java.time.ZoneId;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RomeTaxiPointBuilder extends TupleBuilder {

	/**
	 * The time zone
	 */
	private final ZoneId zone = ZoneId.of("Europe/Berlin");

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RomeTaxiPointBuilder.class);

	// 173;2014-01-04 00:00:07.028304+01;POINT(41.91924450823211 12.5027184734508)
	public Tuple buildTuple(final String keyData, final String valueData) {
		final String[] data = valueData.split(";");

		try {
			final LocalDateTime date = LocalDateTime.parse(data[1].replaceAll("\\+01", "").replaceAll(" ", "T"));

			if(date == null) {
				throw new IllegalArgumentException("Unable to parse: " + data[1]);
			}

			final double dateSeconds = (double) date.atZone(zone).toInstant().toEpochMilli();

			final String[] longLat = data[2].replaceAll("POINT\\(", "").replaceAll("\\)", "").split(" ");
			final double longitude = Double.parseDouble(longLat[0]);
			final double latitude = Double.parseDouble(longLat[1]);

			final Hyperrectangle boundingBox = new Hyperrectangle(dateSeconds, dateSeconds,
					longitude - boxPadding, longitude + boxPadding,
					latitude - boxPadding, latitude + boxPadding);

			final Tuple tuple = new Tuple(keyData, boundingBox, valueData.getBytes());

			return tuple;
		} catch (Exception e) {
			logger.error("Unabe to parse: ", e);
			return null;
		}
	}

}
