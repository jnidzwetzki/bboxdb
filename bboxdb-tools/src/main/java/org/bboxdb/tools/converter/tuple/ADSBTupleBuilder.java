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
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ADSBTupleBuilder extends TupleBuilder {
	
	private class Aircraft {
		
		public final String hexIdent;
		
		public String callsign = null;
		public String altitude = null;
		public String groundSpeed = null;
		public String track = null;
		public String verticalRate = null;
		public double latitude = -1;
		public double longitude = -1;
		public long lastUpdateTimestamp = -1;
		
		public Aircraft(final String hexIdent) {
			this.hexIdent = hexIdent;
		}
		
		/**
		 * Return as GeoJSON String
		 * @return
		 */
		public String toGeoJSON() {
			final int id = Integer.parseInt(hexIdent, 16);
			final GeoJsonPolygon geoJsonPolygon = new GeoJsonPolygon(id);
			geoJsonPolygon.addPoint(longitude, latitude);
			geoJsonPolygon.addProperty("callsign", callsign);
			geoJsonPolygon.addProperty("altitude", altitude);
			geoJsonPolygon.addProperty("groundSpeed", groundSpeed);
			geoJsonPolygon.addProperty("track", track);
			geoJsonPolygon.addProperty("verticalRate", verticalRate);
			geoJsonPolygon.addProperty("lastUpdateTimestamp", Long.toString(lastUpdateTimestamp));

			return geoJsonPolygon.toGeoJson();
		}
		
		public boolean isComplete() {
			return callsign != null && altitude != null && groundSpeed != null && track != null 
					&& verticalRate != null && latitude != -1 && longitude != -1 
					&& lastUpdateTimestamp != -1;
		}

		@Override
		public String toString() {
			return "Aircraft [hexIdent=" + hexIdent + ", callsign=" + callsign + ", altitude=" + altitude
					+ ", groundSpeed=" + groundSpeed + ", track=" + track + ", verticalRate=" + verticalRate
					+ ", latitude=" + latitude + ", longitude=" + longitude + ", lastUpdateTimestamp="
					+ lastUpdateTimestamp + "]";
		}
	}
	
	/**
	 * The aircrafts
	 */
	private final Map<String, Aircraft> aircrafts; 
	
	/**
	 * The date parser
	 */
	private final SimpleDateFormat dateParser;
	
	public ADSBTupleBuilder() {
		this.aircrafts = new HashMap<>();
		this.dateParser = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
		this.dateParser.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ADSBTupleBuilder.class);

	@Override
	public Tuple buildTuple(final String valueData, final String keyData) {
		
		try {
			
			if(! valueData.startsWith("MSG,")) {
				return null;
			}
			
			final String[] data = valueData.split(",");

			if(data.length < 5) {
				throw new IllegalArgumentException("Unable split: " + valueData + " / " + data.length);
			}
			
			// MSG,1 - ES Identification and Category
			// MSG,3 - ES Airborne Position Message
			// MSG,4 - ES Airborne Velocity Message
			final String transmissionType = data[1];
			final Aircraft aircraft = updateAircraft(data, transmissionType);

			// Emit new tuple after receiving 'Airborne Position Message'
			if("4".equals(transmissionType) && aircraft.isComplete()) {

				final Hyperrectangle boundingBox = new Hyperrectangle(aircraft.latitude, aircraft.latitude, 
						aircraft.longitude, aircraft.longitude);
				
				return new Tuple(aircraft.callsign, boundingBox.enlargeByAmount(boxPadding), 
						aircraft.toGeoJSON().getBytes(), aircraft.lastUpdateTimestamp);
			} 
			
			return null;
		} catch (Exception e) {
			logger.error("Unabe to parse: " + valueData, e);
			return null;
		}
	}

	/**
	 * Update the stored aircraft
	 * @param data
	 * @param hexIdent
	 * @return
	 * @throws InputParseException 
	 * @throws ParseException 
	 */
	private Aircraft updateAircraft(final String[] data, final String transmissionType) 
			throws InputParseException, ParseException {
	
		final String hexIdent = data[4];
		
		if(hexIdent == null) {
			throw new IllegalArgumentException("Received message without hexIdent");
		}
		
		final Aircraft aircraft = aircrafts.computeIfAbsent(hexIdent, (k) -> new Aircraft(k));
		
		// Calculate update time
		final String generatedDate = data[6];
		final String generatedTime = data[7];

		final String dateTime = generatedDate + " " + generatedTime;
		final Date date = dateParser.parse(dateTime);
		
		aircraft.lastUpdateTimestamp = date.getTime();
		
		for(int pos = 10; pos <= 16; pos++) {
			
			if(data.length < pos + 1) {
				break;
			}
			
			final String value = data[pos];
			
			if("".equals(value)) {
				continue;
			}
			
			switch(pos) {
				case 10:
					aircraft.callsign = value;
				break;
				case 11:
					aircraft.altitude = value;
				break;
				case 12:
					aircraft.groundSpeed = value;
				break;
				case 13:
					aircraft.track = value;
				break;
				case 14:
					aircraft.latitude = MathUtil.tryParseDouble(value, () -> "Unable to parse latitude");
				break;
				case 15:
					aircraft.longitude = MathUtil.tryParseDouble(value, () -> "Unable to parse longitude");
				break;
				case 16:
					aircraft.verticalRate = value;
				break;
				
				default:
					throw new IllegalArgumentException("Unable to handle element: " + value + " " + pos);
			}
		}
		
		return aircraft;
	}
}
