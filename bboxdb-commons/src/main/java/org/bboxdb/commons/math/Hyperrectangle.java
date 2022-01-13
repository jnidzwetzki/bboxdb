/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
package org.bboxdb.commons.math;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.StringTokenizer;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.StringUtil;
import org.bboxdb.commons.io.DataEncoderHelper;

public class Hyperrectangle implements Comparable<Hyperrectangle> {

	/**
	 * This special bounding box covers every space completely
	 */
	public final static Hyperrectangle FULL_SPACE = new Hyperrectangle();

	/**
	 * The return value of an invalid dimension
	 */
	public final static int INVALID_DIMENSION = -1;

	/**
	 * Enable advanced (expensive and additional) consistency checks
	 */
	public static boolean enableChecks = false;

	/**
	 * The boundingBox contains a interval for each dimension
	 */
	private final double[] boundingBox;

	/**
	 * The points included information
	 */
	private final boolean[] pointIncluded;

	/**
	 * Create from Double
	 * @param args
	 */
	public Hyperrectangle(final Double... args) {

		assert(args.length % 2 == 0) : "Even number of arguments expected";

		this.boundingBox = new double[args.length];
		this.pointIncluded = new boolean[args.length];

		for(int i = 0; i < args.length; i++) {

			boundingBox[i] = args[i];
			pointIncluded[i] = true;

			if(i % 2 == 1 && boundingBox[i - 1] > boundingBox[i]) {
				throw new IllegalArgumentException(boundingBox[i - 1]  +
						" shuould be smaller than " + boundingBox[i]);
			}
		}
	}

	/**
	 * Create from double[]
	 * @param args
	 */
	public Hyperrectangle(final double[] values) {

		if(values.length % 2 != 0) {
			throw new IllegalArgumentException("Even number of arguments expected");
		}

		this.boundingBox = values;
		this.pointIncluded = new boolean[values.length];

		for(int i = 0; i < values.length; i++) {
			pointIncluded[i] = true;
		}
	}

	/**
	 * Create from List<DoubleInterval>
	 * @param args
	 */
	public Hyperrectangle(final List<DoubleInterval> values) {

		final int elements = values.size() * 2;

		this.boundingBox = new double[elements];
		this.pointIncluded = new boolean[elements];

		intervalsToArray(values);
	}

	/**
	 * Convert a list with double intervals into an array
	 * @param values
	 */
	private void intervalsToArray(final List<DoubleInterval> values) {
		for(int i = 0; i < values.size(); i++) {
			final DoubleInterval interval = values.get(i);

			boundingBox[i * 2] = interval.getBegin();
			boundingBox[i * 2 + 1] = interval.getEnd();
			pointIncluded[i * 2] = interval.isBeginIncluded();
			pointIncluded[i * 2 + 1] = interval.isEndIncluded();
		}
	}

	/***
	 * Create a bounding box from a string value
	 * @param stringValue
	 */
	public static Hyperrectangle fromString(final String stringValue) {

		if(! stringValue.startsWith("[")) {
			throw new IllegalArgumentException("Bounding box have to start with [");
		}

		if(! stringValue.endsWith("]")) {
			throw new IllegalArgumentException("Bounding box have to end with ]");
		}


		if("[]".equals(stringValue)) {
			// Cover complete space bounding box
			return FULL_SPACE;
		}

		if(StringUtil.countCharOccurrence(stringValue, ',') < 1) {
			throw new IllegalArgumentException("Bounding box have to contain at least one ','");
		}

		// [[-5.0,5.0]:[-5.0,5.0]]
		final String shortString = stringValue.substring(1, stringValue.length() - 1);
		final StringTokenizer stringTokenizer = new StringTokenizer(shortString, ":");

		final List<DoubleInterval> values = new ArrayList<>();

		while(stringTokenizer.hasMoreTokens()) {
			final String nextToken = stringTokenizer.nextToken();
			final DoubleInterval interval = new DoubleInterval(nextToken);
			values.add(interval);
		}

		return new Hyperrectangle(values);
	}

	/**
	 * Returns the size of the bounding box in bytes
	 *
	 * @return
	 */
	public int getSize() {
		return (boundingBox.length * DataEncoderHelper.DOUBLE_BYTES)
				+ (pointIncluded.length * DataEncoderHelper.BOOLEAN_BYTES);
	}

	/**
	 * Convert the bounding box into a byte array
	 *
	 * @return
	 */
	public byte[] toByteArray() {
		final double[] values = toDoubleArray();

		return DataEncoderHelper.doubleArrayToByteBuffer(values).array();
	}

	/**
	 * Convert the bounding box into a double array
	 *
	 * @return
	 */
	public double[] toDoubleArray() {
		return boundingBox;
	}

	/**
	 * Read the bounding box from a byte array
	 * @param boxBytes
	 * @return
	 */
	public static Hyperrectangle fromByteArray(final byte[] boxBytes) {
		final double[] doubleArray = DataEncoderHelper.readDoubleArrayFromByte(boxBytes);

		if(doubleArray.length == 0) {
			return FULL_SPACE;
		}

		return new Hyperrectangle(doubleArray);
	}

	/**
	 * Create a bounding box the covers the full space for n dimensions
	 * @param dimension
	 * @return
	 */
	public static Hyperrectangle createFullCoveringDimensionBoundingBox(final int dimension) {
		if(dimension <= 0) {
			throw new IllegalArgumentException("Unable to create full covering bounding box for dimension: " + dimension);
		}

		final List<DoubleInterval> dimensions = new ArrayList<>();

		for(int i = 0; i < dimension; i++) {
			dimensions.add(new DoubleInterval(DoubleInterval.MIN_VALUE, DoubleInterval.MAX_VALUE));
		}

		return new Hyperrectangle(dimensions);
	}

	/**
	 * Tests if two bounding boxes share some space
	 * @param otherBoundingBox
	 * @return
	 */
	public boolean intersects(final Hyperrectangle otherBoundingBox) {

		// Null does overlap with nothing
		if(otherBoundingBox == null) {
			return false;
		}

		// The empty bounding box overlaps everything
		if(this == Hyperrectangle.FULL_SPACE) {
			return true;
		}

		// The empty bounding box overlaps everything
		if(otherBoundingBox == Hyperrectangle.FULL_SPACE) {
			return true;
		}

		// An other instance of the bounding box
		if(otherBoundingBox.getDimension() == 0) {
			return true;
		}

		// Both boxes are equal (Case 5)
		if(equals(otherBoundingBox)) {
			return true;
		}

		// Dimensions are not equal
		if(otherBoundingBox.getDimension() != getDimension()) {
			return false;
		}

		// Check the overlapping in each dimension d
		for(int d = 0; d < getDimension(); d++) {

			final DoubleInterval ourInterval = getIntervalForDimension(d);
			final DoubleInterval otherInterval = otherBoundingBox.getIntervalForDimension(d);

			if(! ourInterval.isOverlappingWith(otherInterval)) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Returns a new Hyperrectangle, enlarged on each dimension by a factor
	 * @param amount
	 * @return
	 */
	public Hyperrectangle enlargeByFactor(final double factor) {

		final Double[] enlargement = new Double[getDimension()];
		
		for(int i = 0; i < getDimension(); i++) {
			// Enlarge by half extension per side 			
			enlargement[i] = (getExtent(i) * (factor - 1)) / 2.0;
		}
		
		return addPadding(enlargement);
	}
	
	/**
	 * Returns a new Hyperrectangle, enlarged on each dimension by 2 * amount
	 * @param amount
	 * @return
	 */
	public Hyperrectangle enlargeByAmount(final double amount) {

		final Double[] enlargement = new Double[getDimension()];
		
		for(int i = 0; i < getDimension(); i++) {
			enlargement[i] = amount;
		}
		
		return addPadding(enlargement);
	}
	
	/**
	 * Enlarge the bounding box by meters
	 * @param meterLat
	 * @param meterLon
	 * @return
	 */
	public Hyperrectangle enlargeByMeters(final double meterLat, final double meterLon) {
		
	    final int dimension = getDimension();
		
		if(dimension != 2) {
			throw new IllegalArgumentException("Bounding box has the wrong dimension: " + dimension);
		}
		
		final double latitudeLow = getCoordinateLow(0);
		final double longitudeLow = getCoordinateLow(1);
		final double latitudeHigh = getCoordinateHigh(0);
		final double longitudeHigh = getCoordinateHigh(1);
		
	    //final double lat0 = Math.toRadians(latitudeLow);
	    final double lat0=(latitudeLow * (Math.PI) / 180);

	    // The radius as defined by WGS84
	    final double equator_circumference = 6371000.0;
	    final double polar_circumference = 6356800.0;

	    final double m_per_deg_long = 360 / polar_circumference;
	    final double m_per_deg_lat = Math.abs(360 / (Math.cos(lat0) * equator_circumference));

	    final double deg_diff_lat = meterLat * m_per_deg_lat; 
	    final double deg_diff_long = meterLon * m_per_deg_long;
		
		return new Hyperrectangle(
				latitudeLow - (deg_diff_lat / 2.0), 
				latitudeHigh + (deg_diff_lat / 2.0), 
				longitudeLow - (deg_diff_long / 2.0), 
				longitudeHigh + (deg_diff_long / 2.0));
	}
	
	/**
	 * Add the padding given by the bounding box
	 * @param paddingBox
	 * @return
	 */
	public Hyperrectangle addPadding(final Double... padding) {
		if(getDimension() != padding.length) {
			throw new IllegalArgumentException("Padding does not match dimension");
		}
		
		final List<DoubleInterval> newIntervals = new ArrayList<>(getDimension());

		for(int i = 0; i < getDimension(); i++) {
			final DoubleInterval interval = getIntervalForDimension(i);
			newIntervals.add(new DoubleInterval(interval.getBegin() - padding[i], interval.getEnd() + padding[i]));
		}

		return new Hyperrectangle(newIntervals);
	}

	/**
	 * Does the bounding box covers the point in the dimension?
	 * @param point
	 * @param dimension
	 * @return
	 */
	public boolean isCoveringPointInDimension(final double point, final int dimension) {

		if(dimension >= getDimension()) {
			throw new IllegalArgumentException("Wrong dimension : " + dimension + " we have only " + getDimension() + " dimensions");
		}

		final DoubleInterval dimensionInterval = getIntervalForDimension(dimension);
		return dimensionInterval.isPointIncluded(point);
	}

	/**
	 * Get the extent for the dimension
	 * @param dimension
	 * @return
	 */
	public double getExtent(final int dimension) {
		return getCoordinateHigh(dimension) - getCoordinateLow(dimension);
	}

	/**
	 * Get the double interval for the given dimension
	 * @param dimension
	 * @return
	 */
	public DoubleInterval getIntervalForDimension(final int dimension) {
		return new DoubleInterval(getCoordinateLow(dimension), getCoordinateHigh(dimension),
				pointIncluded[dimension * 2], pointIncluded[dimension * 2 + 1]);
	}

	/**
	 * The the lowest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public double getCoordinateLow(final int dimension) {
		return boundingBox[2 * dimension];
	}

	/**
	 * The the highest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public double getCoordinateHigh(final int dimension) {
		return boundingBox[2 * dimension + 1];
	}

	/**
	 * Return the dimension of the bounding box
	 * @return
	 */
	public int getDimension() {
		return (boundingBox.length / 2);
	}

	/**
	 * Split the bounding box at the given position and get the left part
	 * @param splitPosition
	 * @param splitPositionIncluded
	 * @return
	 */
	public Hyperrectangle splitAndGetLeft(final double splitPosition,
			final int splitDimension, final boolean splitPositionIncluded) {

		if(splitDimension > getDimension() - 1) {
			throw new IllegalArgumentException("Unable to split a bounding box with " + getDimension() + " dimensions in dimension" + splitDimension);
		}

		if(! isCoveringPointInDimension(splitPosition, splitDimension)) {
			throw new IllegalArgumentException("Unable to split, point " + splitPosition + " is not covered in dimension " + splitDimension + " " + getIntervalForDimension(splitDimension));
		}

		final List<DoubleInterval> intervals = new ArrayList<>(getDimension());

		for(int i = 0; i < getDimension(); i++) {
			intervals.add(getIntervalForDimension(i));
		}

		final DoubleInterval splitInterval = intervals.get(splitDimension);
		final DoubleInterval newInterval = splitInterval.splitAndGetLeftPart(splitPosition, splitPositionIncluded);
		intervals.set(splitDimension, newInterval);
		return new Hyperrectangle(intervals);
	}

	/**
	 * Split the bounding box at the given position and get the right part
	 * @param splitPosition
	 * @param splitPositionIncluded
	 * @return
	 */
	public Hyperrectangle splitAndGetRight(final double splitPosition, final int splitDimension, final boolean splitPositionIncluded) {
		if(splitDimension > getDimension()) {
			throw new IllegalArgumentException("Unable to split a bounding box with " + getDimension() + " dimensions in dimension" + splitDimension);
		}

		if(! isCoveringPointInDimension(splitPosition, splitDimension)) {
			throw new IllegalArgumentException("Unable to split, point " + splitDimension + " is not covered in dimension " + splitDimension);
		}

		final List<DoubleInterval> intervals = new ArrayList<>(getDimension());

		for(int i = 0; i < getDimension(); i++) {
			intervals.add(getIntervalForDimension(i));
		}

		final DoubleInterval splitInterval = intervals.get(splitDimension);
		final DoubleInterval newInterval = splitInterval.splitAndGetRightPart(splitPosition, splitPositionIncluded);
		intervals.set(splitDimension, newInterval);
		return new Hyperrectangle(intervals);
	}

	/**
	 * Convert to a readable string
	 *
	 */
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("Hyperrectangle [dimensions=");
		sb.append(getDimension());

		for(int d = 0; d < getDimension(); d++) {
			sb.append(", dimension ");
			sb.append(d);
			sb.append(" low: ");
			sb.append(MathUtil.doubleToString(getCoordinateLow(d)));
			sb.append(" high: ");
			sb.append(MathUtil.doubleToString(getCoordinateHigh(d)));			
		}

		sb.append("]");
		return sb.toString();
	}
	

	/**
	 * Return a compact string that describes the box
	 * @return
	 */
	public String toCompactString() {
		final StringBuilder sb = new StringBuilder("[");
		for(int d = 0; d < getDimension(); d++) {

			if(d != 0) {
				sb.append(":");
			}

			sb.append(getIntervalForDimension(d));
		}
		sb.append("]");

		return sb.toString();
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(boundingBox);
		result = prime * result + Arrays.hashCode(pointIncluded);
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
		Hyperrectangle other = (Hyperrectangle) obj;
		if (!Arrays.equals(boundingBox, other.boundingBox))
			return false;
		if (!Arrays.equals(pointIncluded, other.pointIncluded))
			return false;
		return true;
	}

	/**
	 * Compare to an other bounding box
	 */
	@Override
	public int compareTo(final Hyperrectangle otherBox) {

		// Check number od dimensions
		if(getDimension() != otherBox.getDimension()) {
			return getDimension() - otherBox.getDimension();
		}

		// Check start point of each dimension
		for(int d = 0; d < getDimension(); d++) {
			if(getCoordinateLow(d) != otherBox.getCoordinateLow(d)) {
				if(getCoordinateLow(d) > otherBox.getCoordinateLow(d)) {
					return 1;
				} else {
					return -1;
				}
			}
		}

		// Objects are equal
		return 0;
	}

	/**
	 * Get the bounding box of two bounding boxes
	 *
	 * (run-time optimized version for 2 arguments)
	 *
	 * @param boundingBox1
	 * @param boundingBox2
	 * @return
	 */
	public static Hyperrectangle getCoveringBox(final Hyperrectangle hyperrectangle1,
			final Hyperrectangle hyperrectangle2) {

		if(hyperrectangle1 == FULL_SPACE) {
			return hyperrectangle2;
		}

		if(hyperrectangle2 == FULL_SPACE) {
			return hyperrectangle1;
		}

		if(enableChecks) {
			if(hyperrectangle1.getDimension() != hyperrectangle2.getDimension()) {
				final String errorMessage = "Merging bounding boxes with different dimensions: "
						+ hyperrectangle1 + "/" + hyperrectangle2;

				throw new IllegalArgumentException(errorMessage);
			}
		}

		final int dimensions = hyperrectangle1.getDimension();

		// Array with data for the result box
		final double[] coverBox = new double[dimensions * 2];

		for(int d = 0; d < dimensions; d++) {
			coverBox[2 * d] = Math.min(hyperrectangle1.getCoordinateLow(d), hyperrectangle2.getCoordinateLow(d));
			coverBox[2 * d + 1] = Math.max(hyperrectangle1.getCoordinateHigh(d), hyperrectangle2.getCoordinateHigh(d));
		}

		return new Hyperrectangle(coverBox);
	}

	/**
	 * Get the bounding box of two bounding boxes
	 *
	 *
	 * @param boundingBox1
	 * @param boundingBox2
	 * @return
	 */
	public static Hyperrectangle getCoveringBox(final List<Hyperrectangle> boundingBoxes) {

		final int elements = boundingBoxes.size();

		if(elements == 0) {
			return Hyperrectangle.FULL_SPACE;
		}

		if(elements == 1) {
			return boundingBoxes.get(0);
		}

		final int dimensions = boundingBoxes.get(0).getDimension();

		if(enableChecks) {
			final Optional<Hyperrectangle> result = boundingBoxes.stream()
					.filter(b -> b.getDimension() != dimensions)
					.findAny();

			if(result.isPresent()) {
				final String errorMessage = "Merging bounding boxes with different dimensions: "
						+ dimensions + "/" + result.get().getDimension();

				throw new IllegalArgumentException(errorMessage);
			}
		}

		// Array with data for the result box
		final double[] coverBox = new double[dimensions * 2];

		// Construct the covering bounding box
		for(int d = 0; d < dimensions; d++) {
			final List<Double> values = new ArrayList<>();
			
			for(final Hyperrectangle currentBox : boundingBoxes) {

				if(currentBox == FULL_SPACE) {
					continue;
				}

				values.add(currentBox.getCoordinateLow(d));
				values.add(currentBox.getCoordinateHigh(d));
			}
			
			values.sort(Comparator.naturalOrder());
			
			if(values.isEmpty()) {
				return Hyperrectangle.FULL_SPACE;
			}

			coverBox[2 * d] = values.get(0); // Begin position
			coverBox[2 * d + 1] = values.get(values.size() - 1); // End position
		}

		return new Hyperrectangle(coverBox);
	}


	/**
	 * Is the bounding box fully covered by this bounding box?
	 * @param otherBox
	 * @return
	 */
	public boolean isCovering(final Hyperrectangle otherBox) {
		if(this == FULL_SPACE || otherBox == FULL_SPACE) {
			return true;
		}

		throwExceptionIfDimensionNotMatch(otherBox);

		for(int d = 0; d < getDimension(); d++) {

			if(otherBox.getCoordinateLow(d) < getCoordinateLow(d)) {
				return false;
			}

			if(otherBox.getCoordinateHigh(d) > getCoordinateHigh(d)) {
				return false;
			}

			if(otherBox.getCoordinateLow(d) == getCoordinateLow(d)) {
				if(isLowPointIncluded(d) == false && otherBox.isLowPointIncluded(d) == true) {
					return false;
				}
			}

			if(otherBox.getCoordinateHigh(d) == getCoordinateHigh(d)) {
				if(isHighPointIncluded(d) == false && otherBox.isHighPointIncluded(d) == true) {
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Is the low point included
	 * @param dimension
	 * @return
	 */
	public boolean isLowPointIncluded(final int dimension) {
		return pointIncluded[dimension * 2];
	}

	/**
	 * Is the high point included
	 * @param dimension
	 * @return
	 */
	public boolean isHighPointIncluded(final int dimension) {
		return pointIncluded[dimension * 2 + 1];
	}


	/**
	 * Calculated the needed space for the enlargement to cover the other box
	 * @param otherBox
	 * @return
	 */
	public double calculateEnlargement(final Hyperrectangle otherBox) {
		if(this == FULL_SPACE || otherBox == FULL_SPACE) {
			return 0;
		}

		throwExceptionIfDimensionNotMatch(otherBox);

		if(isCovering(otherBox)) {
			return 0;
		}

		final Hyperrectangle mergedBox = getCoveringBox(this, otherBox);

		final double ourVolume = getVolume();

		return mergedBox.getVolume() - ourVolume;
	}

	/**
	 * Get the volume of the box
	 * @return
	 */
	public double getVolume() {

		double volume = 1;

		for(int d = 0; d < getDimension(); d++) {
			final double extend = getCoordinateHigh(d) - getCoordinateLow(d);
			volume = volume * extend;
		}

		return volume;
	}
	
	/**
	 * Scale up the volume by the given percentage (e.g, 1.2 for 120%)
	 * @param percentage
	 * @return
	 */
	public Hyperrectangle scaleVolumeByPercentage(final double percentage) {
		
		// Don't scale the full space
		if(this == FULL_SPACE) {
			return null;
		}

		// Don't scale rectangles with a min or max value
		for(double value : boundingBox) {
			if(value == DoubleInterval.MIN_VALUE) {
				return null;
			}
			
			
			if(value == DoubleInterval.MAX_VALUE) {
				return null;
			}
		}
		
		final double factor = Math.pow(percentage, 1.0 / getDimension());
		
		return enlargeByFactor(factor);
	}

	/**
	 * Get the intersection of this and another bounding box
	 * @param otherBox
	 * @return
	 */
	public Hyperrectangle getIntersection(final Hyperrectangle otherBox) {

		if(getDimension() == 0 || otherBox.getDimension() == 0) {
			return FULL_SPACE;
		}

		throwExceptionIfDimensionNotMatch(otherBox);

		final List<DoubleInterval> intervalList = new ArrayList<DoubleInterval>();

		// Process dimensions
		for(int d = 0; d < getDimension(); d++) {
			final DoubleInterval ourInterval = getIntervalForDimension(d);
			final DoubleInterval otherInterval = otherBox.getIntervalForDimension(d);
			final DoubleInterval intersection = ourInterval.getIntersection(otherInterval);

			if(intersection == null) {
				return FULL_SPACE;
			}

			intervalList.add(intersection);
		}

		return new Hyperrectangle(intervalList);
	}

	/**
	 * Throw an exception of the dimension of the other box don't match
	 * @param otherBox
	 */
	protected void throwExceptionIfDimensionNotMatch(final Hyperrectangle otherBox) {

		if(getDimension() != otherBox.getDimension()) {
			throw new IllegalArgumentException(
					"Unable to perform operation for boundig boxes with differnet dimensions: "
					+ getDimension() + " " + otherBox.getDimension());
		}

	}

	/**
	 * Does the hyperrectangle covers at least one dimension of the
	 * argument completely?
	 * @param bbox
	 * @return
	 */
	public boolean coversAtLeastOneDimensionComplete(final Hyperrectangle hyperrectangle) {
		
		if(this == FULL_SPACE || hyperrectangle == FULL_SPACE) {
			return true;
		}
		
		if(hyperrectangle.getDimension() != getDimension()) {
			throw new IllegalArgumentException("The dimensions of the hyperrectangles needs to be equal: " 
					+ hyperrectangle.getDimension() + " / " + getDimension());
		}
		

		for(int d = 0; d < getDimension(); d++) {
			final DoubleInterval doubleInterval1 = getIntervalForDimension(d);
			final DoubleInterval doubleInterval2 = hyperrectangle.getIntervalForDimension(d);
			
			if(doubleInterval1.isCovering(doubleInterval2)) {
				return true;
			}
		}
		
		return false;
	}

}
