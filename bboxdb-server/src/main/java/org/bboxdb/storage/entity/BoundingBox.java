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
package org.bboxdb.storage.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.util.DataEncoderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BoundingBox implements Comparable<BoundingBox> {
	
	public final static BoundingBox EMPTY_BOX = new BoundingBox();
	
	/**
	 * The boundingBox contains a interval for each dimension
	 */
	protected final List<DoubleInterval> boundingBox;
	
	/**
	 * The return value of an invalid dimension
	 */
	public final static int INVALID_DIMENSION = -1;
	
	/**
	 * The min value
	 */
	public final static double MIN_VALUE = -Float.MAX_VALUE;
	
	/**
	 * The max value
	 */
	public final static double MAX_VALUE = Float.MAX_VALUE;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(BoundingBox.class);
	
	/**
	 * Create from Double
	 * @param args
	 */
	public BoundingBox(final Double... args) {
		
		if(args.length % 2 != 0) {
			throw new IllegalArgumentException("Even number of arguments expected");
		}
		
		boundingBox = new ArrayList<DoubleInterval>(args.length / 2);
				
		for(int i = 0; i < args.length; i = i + 2) {
			final DoubleInterval interval = new DoubleInterval(args[i], args[i+1]);
			boundingBox.add(interval);
		}				
	}
	
	/**
	 * Create from double[]
	 * @param args
	 */
	public BoundingBox(final double[] values) {
		
		if(values.length % 2 != 0) {
			throw new IllegalArgumentException("Even number of arguments expected");
		}
		
		boundingBox = new ArrayList<DoubleInterval>(values.length / 2);
		
		for(int i = 0; i < values.length; i = i + 2) {
			final DoubleInterval interval = new DoubleInterval(values[i], values[i+1]);
			boundingBox.add(interval);
		}				
	}
	
	/**
	 * Create from List<DoubleInterval>
	 * @param args
	 */
	public BoundingBox(final List<DoubleInterval> values) {
		boundingBox = new ArrayList<DoubleInterval>(values);
	}

	/**
	 * Returns the size of the bounding box in bytes
	 * 
	 * @return
	 */
	public int getSize() {
		return boundingBox.size();
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
		final double[] values = new double[boundingBox.size() * 2];
		for(int i = 0; i < boundingBox.size(); i++) {
			values[2*i] = boundingBox.get(i).getBegin();
			values[(2*i)+1] = boundingBox.get(i).getEnd();
		}
		return values;
	}
	
	/**
	 * Read the bounding box from a byte array
	 * @param boxBytes
	 * @return
	 */
	public static BoundingBox fromByteArray(final byte[] boxBytes) {
		final double[] doubleArray = DataEncoderHelper.readDoubleArrayFromByte(boxBytes);
		return new BoundingBox(doubleArray);
	}
	
	/**
	 * Create a bounding box the covers the full space for n dimensions
	 * @param dimension
	 * @return
	 */
	public static BoundingBox createFullCoveringDimensionBoundingBox(final int dimension) {
		if(dimension <= 0) {
			throw new IllegalArgumentException("Unable to create full covering bounding box for dimension: " + dimension);
		}
		
		final List<DoubleInterval> dimensions = new ArrayList<DoubleInterval>();
		
		for(int i = 0; i < dimension; i++) {
			dimensions.add(new DoubleInterval(MIN_VALUE, MAX_VALUE));
		}
		
		return new BoundingBox(dimensions);
	}
	
	/**
	 * Tests if two bounding boxes share some space
	 * @param otherBoundingBox
	 * @return
	 */
	public boolean overlaps(final BoundingBox otherBoundingBox) {
		
		// Null does overlap with nothing
		if(otherBoundingBox == null) {
			return false;
		}
		
		// The empty bounding box overlaps everything
		if(this == BoundingBox.EMPTY_BOX) {
			return true;
		}
		
		// The empty bounding box overlaps everything
		if(otherBoundingBox == BoundingBox.EMPTY_BOX) {
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
			
			final DoubleInterval ourInterval = boundingBox.get(d);
			final DoubleInterval otherInterval = otherBoundingBox.getIntervalForDimension(d);
			
			if(! ourInterval.isOverlappingWith(otherInterval)) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Does the bounding box covers the point in the dimension?
	 * @param point
	 * @param dimension
	 * @return
	 */
	public boolean isCoveringPointInDimension(final double point, final int dimension) {
		
		if(dimension >= boundingBox.size()) {
			throw new IllegalArgumentException("Wrong dimension : " + dimension + " we have only " + boundingBox.size() + " dimensions");
		}
		
		final DoubleInterval dimensionInterval = boundingBox.get(dimension);
		return dimensionInterval.isNumberIncluded(point);
	}
	
	/**
	 * Get the extent for the dimension
	 * @param dimension
	 * @return
	 */
	public double getExtent(final int dimension) {
		return boundingBox.get(dimension).getEnd() - boundingBox.get(dimension).getBegin();
	}
	
	/**
	 * Get the double interval for the given dimension
	 * @param dimension
	 * @return
	 */
	public DoubleInterval getIntervalForDimension(final int dimension) {
		return boundingBox.get(dimension);
	}
	
	/**
	 * The the lowest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public double getCoordinateLow(final int dimension) {
		return boundingBox.get(dimension).getBegin();
	}
	
	/**
	 * The the highest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public double getCoordinateHigh(final int dimension) {
		return boundingBox.get(dimension).getEnd();
	}
	
	/**
	 * Return the dimension of the bounding box
	 * @return
	 */
	public int getDimension() {
		return boundingBox.size();
	}
	
	/**
	 * Split the bounding box at the given position and get the left part
	 * @param splitPosition
	 * @param splitPositionIncluded
	 * @return
	 */
	public BoundingBox splitAndGetLeft(final double splitPosition, 
			final int splitDimension, final boolean splitPositionIncluded) {
		
		if(splitDimension > getDimension() - 1) {
			throw new IllegalArgumentException("Unable to split a bounding box with " + getDimension() + " dimensions in dimension" + splitDimension);
		}
		
		if(! isCoveringPointInDimension(splitPosition, splitDimension)) {
			throw new IllegalArgumentException("Unable to split, point " + splitPosition + " is not covered in dimension " + splitDimension + " " + boundingBox.get(splitDimension));
		}
		
		final List<DoubleInterval> intervals = new ArrayList<DoubleInterval>(boundingBox);
		final DoubleInterval splitInterval = intervals.get(splitDimension);
		final DoubleInterval newInterval = splitInterval.splitAndGetLeftPart(splitPosition, splitPositionIncluded);
		intervals.set(splitDimension, newInterval);
		return new BoundingBox(intervals);
	}
	
	/**
	 * Split the bounding box at the given position and get the right part
	 * @param splitPosition
	 * @param splitPositionIncluded
	 * @return
	 */
	public BoundingBox splitAndGetRight(final double splitPosition, final int splitDimension, final boolean splitPositionIncluded) {
		if(splitDimension > getDimension()) {
			throw new IllegalArgumentException("Unable to split a bounding box with " + getDimension() + " dimensions in dimension" + splitDimension);
		}
		
		if(! isCoveringPointInDimension(splitPosition, splitDimension)) {
			throw new IllegalArgumentException("Unable to split, point " + splitDimension + " is not covered in dimension " + splitDimension);
		}
		
		final List<DoubleInterval> intervals = new ArrayList<DoubleInterval>(boundingBox);
		final DoubleInterval splitInterval = intervals.get(splitDimension);
		final DoubleInterval newInterval = splitInterval.splitAndGetRightPart(splitPosition, splitPositionIncluded);
		intervals.set(splitDimension, newInterval);
		return new BoundingBox(intervals);
	}

	/**
	 * Convert to a readable string
	 * 
	 */
	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("BoundingBox [dimensions=");
		sb.append(getDimension());
		
		for(int d = 0; d < getDimension(); d++) {
			sb.append(", dimension ");
			sb.append(d);
			sb.append(" low: ");
			sb.append(getCoordinateLow(d));
			sb.append(" high: ");
			sb.append(getCoordinateHigh(d));
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
				sb.append(",");
			}
			
			sb.append(getCoordinateLow(d));
			sb.append(":");
			sb.append(getCoordinateHigh(d));
		}
		sb.append("]");
		
		return sb.toString();
	}

	/**
	 * Convert into a hashcode
	 * 
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((boundingBox == null) ? 0 : boundingBox.hashCode());
		return result;
	}

	/**
	 * Is equals with an other object
	 * 
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BoundingBox other = (BoundingBox) obj;
		if (boundingBox == null) {
			if (other.boundingBox != null)
				return false;
		} else if (!boundingBox.equals(other.boundingBox))
			return false;
		return true;
	}

	/**
	 * Compare to an other boudning box
	 */
	@Override
	public int compareTo(final BoundingBox otherBox) {
		
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
	 * @param boundingBox1
	 * @param boundingBox2
	 * @return
	 */
	public static BoundingBox getCoveringBox(final BoundingBox... boundingBoxes) {
		return getCoveringBox(Arrays.asList(boundingBoxes));
	}
	
	/**
	 * Get the bounding box of two bounding boxes
	 * @param boundingBox1
	 * @param boundingBox2
	 * @return
	 */
	public static BoundingBox getCoveringBox(final List<BoundingBox> argumentBoundingBoxes) {

		// Bounding box could be null, e.g. for DeletedTuple instances.
		// And don't merge empty boxes
		final List<BoundingBox> boundingBoxes = argumentBoundingBoxes
				.stream()
				.filter(b -> b != null)
				.filter(b -> b != EMPTY_BOX)
				.collect(Collectors.toList());
		
		// No argument
		if(boundingBoxes.isEmpty()) {
			return BoundingBox.EMPTY_BOX;
		}
		
		// Only 1 argument
		if(boundingBoxes.size() == 1) {
			return boundingBoxes.get(0);
		}
		
		final int dimensions = boundingBoxes.get(0).getDimension();
		
		// All bounding boxes need the same dimension
		for(final BoundingBox currentBox : boundingBoxes) {
			if(dimensions != currentBox.getDimension()) {
				final String errorMessage = "Merging bounding boxes with different dimensions: " 
						+ dimensions + "/" + currentBox.getDimension();
				
				throw new IllegalArgumentException(errorMessage);
			}
		}
		
		// Array with data for the result box
		final double[] coverBox = new double[dimensions * 2];
		
		// Construct the covering bounding box
		for(int d = 0; d < dimensions; d++) {
			double resultMin = Double.MAX_VALUE;
			double resultMax = Double.MIN_VALUE;
			
			for(final BoundingBox currentBox : boundingBoxes) {
				resultMin = Math.min(resultMin, currentBox.getCoordinateLow(d));
				resultMax = Math.max(resultMax, currentBox.getCoordinateHigh(d));
			}
			
			coverBox[2 * d] = resultMin;     // Begin position
			coverBox[2 * d + 1] = resultMax; // End position
		}
		
		return new BoundingBox(coverBox);
	}

	
	/**
	 * Is the bounding box fully covered by this bounding box?
	 * @param otherBox
	 * @return
	 */
	public boolean isCovering(final BoundingBox otherBox) {
		if(this == EMPTY_BOX || otherBox == EMPTY_BOX) {
			return true;
		}
	
		throwExceptionIfDimensionNotMatch(otherBox);
		
		for(int d = 0; d < getDimension(); d++) {
			if(! getIntervalForDimension(d).isCovering(otherBox.getIntervalForDimension(d))) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Calculated the needed space for the enlargement to cover the other box
	 * @param otherBox
	 * @return
	 */
	public double calculateEnlargement(final BoundingBox otherBox) {
		if(this == EMPTY_BOX || otherBox == EMPTY_BOX) {
			return 0;
		}
		
		throwExceptionIfDimensionNotMatch(otherBox);

		if(isCovering(otherBox)) {
			return 0;
		}
		
		final double ourVolume = getVolume();
		
		final BoundingBox mergedBox = BoundingBox.getCoveringBox(this, otherBox);
		
		return mergedBox.getVolume() - ourVolume;
	}
	
	/**
	 * Get the volume of the box
	 * @return
	 */
	public double getVolume() {
		
		double volume = 1;
		
		for(int d = 0; d < getDimension(); d++) {
			final DoubleInterval dimension = getIntervalForDimension(d);
			final double extend = dimension.getEnd() - dimension.getBegin();
			volume = volume * extend;
		}
		
		return volume;
	}
	
	/**
	 * Get the intersection of this and another bounding box
	 * @param otherBox
	 * @return 
	 */
	public BoundingBox getIntersection(final BoundingBox otherBox) {
		
		if(getDimension() == 0 || otherBox.getDimension() == 0) {
			return EMPTY_BOX;
		}
		
		throwExceptionIfDimensionNotMatch(otherBox);
		
		final List<DoubleInterval> intervalList = new ArrayList<DoubleInterval>();
		
		// Process dimensions
		for(int d = 0; d < getDimension(); d++) {
			final DoubleInterval ourInterval = getIntervalForDimension(d);
			final DoubleInterval otherInterval = otherBox.getIntervalForDimension(d);
			final DoubleInterval intersection = ourInterval.getIntersection(otherInterval);

			if(intersection == null) {
				return EMPTY_BOX;
			}
			
			intervalList.add(intersection);
		}
		
		return new BoundingBox(intervalList);
	}

	/**
	 * Throw an exception of the dimension of the other box don't match
	 * @param otherBox
	 */
	protected void throwExceptionIfDimensionNotMatch(final BoundingBox otherBox) {
		
		if(getDimension() != otherBox.getDimension()) {
			throw new IllegalArgumentException(
					"Unable to calculate intersection for boundig boxes with differnet dimensions: "
					+ getDimension() + " " + otherBox.getDimension());
		}
		
	}
	
}
