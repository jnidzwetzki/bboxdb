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
import java.util.TimeZone;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;

public class BerlinModTupleBuilder extends TupleBuilder {
	
	/**
	 * The date parser
	 */
	private final SimpleDateFormat dateParser;
	
	public BerlinModTupleBuilder() {
		this.dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		this.dateParser.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	@Override
	public Tuple buildTuple(final String valueData, final String keyData) {
		
		// Ignore the header
		if("Moid,Tripid,Tstart,Tend,Xstart,Ystart,Xend,Yend".equals(valueData)) {
			return null;
		}

		// 2000,292882,2007-05-27,2007-05-28 09:00:34.446,13.327,52.4981,13.327,52.4981
		//Moid,Tripid,Tstart,Tend,Xstart,Ystart,Xend,Yend
		final String[] values = valueData.split(",");
		
		if(values.length != 8) {
			throw new RuntimeException("Unable to decode tuple: " + valueData);
		}
		
		final String key = keyData != null ? keyData : values[0];
		final long polygonId = MathUtil.tryParseLongOrExit(key, () -> "Unale to parse: " + key);
		
		final GeoJsonPolygon polygon = new GeoJsonPolygon(polygonId);
		final String latString1 = values[4];
		final String lonString1 = values[5];
		final String latString2 = values[6];
		final String lonString2 = values[7];

		final double lat1 = MathUtil.tryParseDoubleOrExit(latString1, () -> "Unable to parse lat: " + latString1);
		final double lon1 = MathUtil.tryParseDoubleOrExit(lonString1, () -> "Unable to parse lon: " + lonString1);
		final double lat2 = MathUtil.tryParseDoubleOrExit(latString2, () -> "Unable to parse lat: " + latString2);
		final double lon2 = MathUtil.tryParseDoubleOrExit(lonString2, () -> "Unable to parse lon: " + lonString2);
		
		polygon.addPoint(lon1, lat1);
		polygon.addPoint(lon2, lat2);
		
		polygon.addProperty("MOID", key);
		polygon.addProperty("TRIP", values[1]);

		final byte[] tupleBytes = polygon.toGeoJson().getBytes();

		if(polygon.getBoundingBox().getDimension() == 0) {
			return null;
		}
		
		try {
			final Date date = dateParser.parse(values[3]);
			final Hyperrectangle enlargedBox = polygon.getBoundingBox().enlargeByAmount(boxPadding);
			return new Tuple(key, enlargedBox, tupleBytes, date.getTime());
		} catch (ParseException e) {
			throw new RuntimeException("Unable to decode tuple: " + valueData, e);
		}
	}

}
