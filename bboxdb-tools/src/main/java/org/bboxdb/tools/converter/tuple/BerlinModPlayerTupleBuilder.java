/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
import java.util.TimeZone;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;

public class BerlinModPlayerTupleBuilder extends TupleBuilder {
	
	/**
	 * The date parser
	 */
	private final SimpleDateFormat dateParser;
	
	public BerlinModPlayerTupleBuilder() {
		this.dateParser = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
		this.dateParser.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	@Override
	public Tuple buildTuple(final String valueData, final String keyData) {
		
		// Ignore the header
		if("Moid,Tripid,Tstart,Tend,Xstart,Ystart,Xend,Yend".equals(valueData)) {
			return null;
		}

		// 28-05-2007 06:02:16,272,14773,13.2983,52.5722
		// Date, Moid, Tripid, Long, Lat
		final String[] values = valueData.split(",");
		
		if(values.length != 5) {
			throw new RuntimeException("Unable to decode tuple: " + valueData);
		}
		
		final String key = keyData != null ? keyData : values[1];
		final long polygonId = MathUtil.tryParseLongOrExit(key, () -> "Unale to parse: " + key);
		
		final GeoJsonPolygon polygon = new GeoJsonPolygon(polygonId);
		final String latString = values[3];
		final String lonString = values[4];

		final double lat = MathUtil.tryParseDoubleOrExit(latString, () -> "Unable to parse lat: " + latString);
		final double lon = MathUtil.tryParseDoubleOrExit(lonString, () -> "Unable to parse lon: " + lonString);

		polygon.addPoint(lon, lat);
		polygon.addProperty("MOID", key);
		polygon.addProperty("TRIP", values[2]);

		final byte[] tupleBytes = polygon.toGeoJson().getBytes();

		if(polygon.getBoundingBox().getDimension() == 0) {
			return null;
		}
		
		try {
			final Date date = dateParser.parse(values[0]);
			final Hyperrectangle enlargedBox = polygon.getBoundingBox().enlargeByAmount(boxPadding);
			return new Tuple(key, enlargedBox, tupleBytes, date.getTime());
		} catch (ParseException e) {
			throw new RuntimeException("Unable to decode tuple: " + valueData, e);
		}
	}

}
