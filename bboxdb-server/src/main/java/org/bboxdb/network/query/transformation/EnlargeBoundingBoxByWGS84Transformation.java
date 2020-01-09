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
package org.bboxdb.network.query.transformation;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.network.query.entity.TupleAndBoundingBox;

public class EnlargeBoundingBoxByWGS84Transformation implements TupleTransformation {
	
	/**
	 * The change in the latitude
	 */
	private final double meterLat;
	
	/**
	 * The change in the latitude
	 */
	private final double meterLon;
	
	/**
     * The equatorial radius as defined by WGS84
     */
    public static final double EQUATORIAL_RADIUS = 6378137.0;

	public EnlargeBoundingBoxByWGS84Transformation(final double meterLat, final double meterLon) {
		this.meterLat = meterLat;
		this.meterLon = meterLon;
	}
	
	public EnlargeBoundingBoxByWGS84Transformation(final String data) throws InputParseException {
		final String[] splitData = data.split(",");
		
		if(splitData.length != 2) {
			throw new InputParseException("Invalid input data:" + data);
		}
		
		this.meterLat = MathUtil.tryParseDouble(splitData[0], () -> "Unable to parse: " + splitData[0]);
		this.meterLon = MathUtil.tryParseDouble(splitData[1], () -> "Unable to parse: " + splitData[1]);
	}

	@Override
	public TupleAndBoundingBox apply(final TupleAndBoundingBox input) {
		
		final int dimension = input.getBoundingBox().getDimension();
		
		if(dimension != 2) {
			throw new IllegalArgumentException("Bounding box has the wrong dimension: " + dimension);
		}
		
		final Hyperrectangle inputBox = input.getBoundingBox();
		
		final double latitudeLow = inputBox.getCoordinateLow(0);
		final double lonitudeLow = inputBox.getCoordinateLow(1);
		final double latitudeHigh = inputBox.getCoordinateHigh(0);
		final double lonitudeHigh = inputBox.getCoordinateHigh(1);
		
		double latChange = (meterLat / 2 * 360) / (2 * Math.PI * EQUATORIAL_RADIUS);
		double longChange = (meterLon / 2 * 360) / (2 * Math.PI * EQUATORIAL_RADIUS * Math.cos(Math.toRadians(latitudeLow)));

		final Hyperrectangle resultBox = new Hyperrectangle(
				latitudeLow - latChange, 
				latitudeHigh + latChange, 
				lonitudeLow - longChange, 
				lonitudeHigh + longChange);
		
		return new TupleAndBoundingBox(input.getTuple(), resultBox);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(meterLat);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(meterLon);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EnlargeBoundingBoxByWGS84Transformation other = (EnlargeBoundingBoxByWGS84Transformation) obj;
		if (Double.doubleToLongBits(meterLat) != Double.doubleToLongBits(other.meterLat))
			return false;
		if (Double.doubleToLongBits(meterLon) != Double.doubleToLongBits(other.meterLon))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "EnlargeBoundingBoxByWGS84Transformation [meterLat=" + meterLat + ", meterLon=" + meterLon + "]";
	}

	@Override
	public String getSerializedData() {
		return Double.toString(meterLat) + "," + Double.toString(meterLon);
	}
	
}
