package de.fernunihagen.dna.jkn.scalephant.storage.entity;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableHelper;

public class BoundingBox implements Comparable<BoundingBox> {
	
	public final static BoundingBox EMPTY_BOX = new BoundingBox();
	
	/**
	 * The boundingBox contains a interval for each dimension
	 */
	protected final List<FloatInterval> boundingBox;
	
	/**
	 * The return value of an invalid dimension
	 */
	public final static int INVALID_DIMENSION = -1;
	
	/**
	 * The min value
	 */
	public final static float MIN_VALUE = Float.MIN_VALUE;
	
	/**
	 * The max value
	 */
	public final static float MAX_VALUE = Float.MAX_VALUE;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(BoundingBox.class);
	
	/**
	 * Create from Float
	 * @param args
	 */
	public BoundingBox(final Float... args) {
		
		if(args.length % 2 != 0) {
			throw new IllegalArgumentException("Even number of arguments expected");
		}
		
		boundingBox = new ArrayList<FloatInterval>(args.length / 2);
				
		for(int i = 0; i < args.length; i = i + 2) {
			final FloatInterval interval = new FloatInterval(args[i], args[i+1]);
			boundingBox.add(interval);
		}				
	}
	
	/**
	 * Create from float[]
	 * @param args
	 */
	public BoundingBox(final float[] values) {
		
		if(values.length % 2 != 0) {
			throw new IllegalArgumentException("Even number of arguments expected");
		}
		
		boundingBox = new ArrayList<FloatInterval>(values.length / 2);
		
		for(int i = 0; i < values.length; i = i + 2) {
			final FloatInterval interval = new FloatInterval(values[i], values[i+1]);
			boundingBox.add(interval);
		}				
	}
	
	/**
	 * Create from List<FloatInterval>
	 * @param args
	 */
	public BoundingBox(final List<FloatInterval> values) {
		boundingBox = new ArrayList<FloatInterval>(values);
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
		final float[] values = toFloatArray();
		
		return SSTableHelper.floatArrayToIEEE754ByteBuffer(values).array();
	}

	/**
	 * Convert the boudning box into a float array
	 * 
	 * @return
	 */
	public float[] toFloatArray() {
		final float[] values = new float[boundingBox.size() * 2];
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
		final float[] floatArray = SSTableHelper.readIEEE754FloatArrayFromByte(boxBytes);
		return new BoundingBox(floatArray);
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
		
		final List<FloatInterval> dimensions = new ArrayList<FloatInterval>();
		
		for(int i = 0; i < dimension; i++) {
			dimensions.add(new FloatInterval(MIN_VALUE, MAX_VALUE));
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
		if(otherBoundingBox == BoundingBox.EMPTY_BOX) {
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
			
			final FloatInterval ourInterval = boundingBox.get(d);
			final FloatInterval otherInterval = otherBoundingBox.getIntervalForDimension(d);
			
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
	public boolean isCoveringPointInDimension(final float point, final int dimension) {
		
		if(dimension > boundingBox.size()) {
			throw new IllegalArgumentException("Wrong dimension : " + dimension + " we have only" + boundingBox.size() + " dimensions");
		}
		
		final FloatInterval dimensionInterval = boundingBox.get(dimension);
		return dimensionInterval.isNumberIncluded(point);
	}
	
	/**
	 * Get the extent for the dimension
	 * @param dimension
	 * @return
	 */
	public float getExtent(final int dimension) {
		return boundingBox.get(dimension).getEnd() - boundingBox.get(dimension).getBegin();
	}
	
	/**
	 * Get the float interval for the given dimension
	 * @param dimension
	 * @return
	 */
	public FloatInterval getIntervalForDimension(final int dimension) {
		return boundingBox.get(dimension);
	}
	
	/**
	 * The the lowest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public float getCoordinateLow(final int dimension) {
		return boundingBox.get(dimension).getBegin();
	}
	
	/**
	 * The the highest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public float getCoordinateHigh(final int dimension) {
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
	public BoundingBox splitAndGetLeft(final float splitPosition, final int splitDimension, final boolean splitPositionIncluded) {
		if(splitDimension > getDimension() - 1) {
			throw new IllegalArgumentException("Unable to split a bounding box with " + getDimension() + " dimensions in dimension" + splitDimension);
		}
		
		if(! isCoveringPointInDimension(splitPosition, splitDimension)) {
			throw new IllegalArgumentException("Unable to split, point " + splitDimension + " is not covered in dimension " + splitDimension);
		}
		
		final List<FloatInterval> intervals = new ArrayList<FloatInterval>(boundingBox);
		final FloatInterval splitInterval = intervals.get(splitDimension);
		final FloatInterval newInterval = splitInterval.splitAndGetLeftPart(splitPosition, splitPositionIncluded);
		intervals.set(splitDimension, newInterval);
		return new BoundingBox(intervals);
	}
	
	/**
	 * Split the bounding box at the given position and get the right part
	 * @param splitPosition
	 * @param splitPositionIncluded
	 * @return
	 */
	public BoundingBox splitAndGetRight(final float splitPosition, final int splitDimension, final boolean splitPositionIncluded) {
		if(splitDimension > getDimension()) {
			throw new IllegalArgumentException("Unable to split a bounding box with " + getDimension() + " dimensions in dimension" + splitDimension);
		}
		
		if(! isCoveringPointInDimension(splitPosition, splitDimension)) {
			throw new IllegalArgumentException("Unable to split, point " + splitDimension + " is not covered in dimension " + splitDimension);
		}
		
		final List<FloatInterval> intervals = new ArrayList<FloatInterval>(boundingBox);
		final FloatInterval splitInterval = intervals.get(splitDimension);
		final FloatInterval newInterval = splitInterval.splitAndGetRightPart(splitPosition, splitPositionIncluded);
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
	public static BoundingBox getBoundingBox(final BoundingBox... boundingBoxes) {
		
		// No argument
		if(boundingBoxes.length == 0) {
			return null;
		}
		
		// Only 1 argument
		if(boundingBoxes.length == 1) {
			return boundingBoxes[0];
		}
		
		int dimensions = boundingBoxes[0].getDimension();
		
		// All bounding boxes need the same dimension
		for(int i = 1 ; i < boundingBoxes.length; i++) {
			
			final BoundingBox curentBox = boundingBoxes[i];
			
			// Bounding box could be null, e.g. for DeletedTuple instances
			if(curentBox == null) {
				continue;
			}
			
			if(dimensions != curentBox.getDimension()) {
				logger.error("Merging bounding boxes with different dimensions: " + dimensions + "/" + curentBox.getDimension());
				logger.error("Box 0: " + boundingBoxes[0]);
				logger.error("Other box: " + curentBox);
				return null;
			}
		}
		
		// Array with data for the result box
		final float[] coverBox = new float[boundingBoxes[0].getDimension() * 2];
		
		// Construct the covering bounding box
		for(int d = 0; d < dimensions; d++) {
			float resultMin = Float.MAX_VALUE;
			float resultMax = Float.MIN_VALUE;
			
			for(int i = 0; i < boundingBoxes.length; i++) {
				resultMin = Math.min(resultMin, boundingBoxes[i].getCoordinateLow(d));
				resultMax = Math.max(resultMax, boundingBoxes[i].getCoordinateHigh(d));
			}
			
			coverBox[2 * d] = resultMin;     // Begin position
			coverBox[2 * d + 1] = resultMax; // End position
		}
		
		return new BoundingBox(coverBox);
	}
	
}
