package org.bboxdb.tools.converter.tuple;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;

public class ADSBTupleBuilder3D extends ADSBTupleBuilder2D {

	/**
	 * Get the hyperrectangle from the aircraft (3d version)
	 * @param aircraft
	 * @return
	 * @throws InputParseException 
	 */
	@Override
	protected Hyperrectangle getHyperrectangleFromAircraft(final Aircraft aircraft) throws InputParseException {
		
		final double altitudeDouble = MathUtil.tryParseDouble(aircraft.altitude, () -> "Unable to parse altitude: " + aircraft.altitude);
		
		return new Hyperrectangle(aircraft.latitude, aircraft.latitude, 
				aircraft.longitude, aircraft.longitude, altitudeDouble, altitudeDouble);
	}
}
