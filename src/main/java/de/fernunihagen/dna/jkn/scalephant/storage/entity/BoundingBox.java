package de.fernunihagen.dna.jkn.scalephant.storage.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableHelper;

public class BoundingBox {
	
	public final static BoundingBox EMPTY_BOX = new BoundingBox();
	
	/**
	 * The boundingBox contains a bounding box for a tuple.
	 * The boundingBox for n dimensions is structured as follows:
	 * 
	 * boundingBox[0] = coordinate_0
	 * boundingBox[1] = extent_0
	 * boundingBox[2] = coordinate_1
	 * boundingBox[3] = extent_1
	 * boundingBox[4] = coordinate_2
	 * boundingBox[5] = extent_2
	 * 
	 * [...]
	 * boundingBox[2n] = coordinate_n
	 * boundingBox[2n+1] = extent_n
	 */
	protected final List<Float> boundingBox;
	
	public final static int INVALID_DIMENSION = -1;
	
	/**
	 * Is the bounding box valid?
	 */
	protected final boolean valid;
	
	public BoundingBox(Float... args) {
		boundingBox = new ArrayList<Float>(args.length);
		boundingBox.addAll(Arrays.asList(args));
		valid = checkValid();
	}
	
	public BoundingBox(float[] values) {
		boundingBox = new ArrayList<Float>(values.length);
		
		for(int i = 0; i < values.length; i++) {
			boundingBox.add(values[i]);
		}
		
		valid = checkValid();
	}

	/**
	 * Determines if the bounding box is valid or not
	 */
	protected boolean checkValid() {
		
		if (boundingBox.size() % 2 != 0) {
			return false;
		}
		
		// No negative extent
		for(int i = 1; i < boundingBox.size(); i=i+2) {
			if(boundingBox.get(i) < 0) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Returns the valid state of the bounding box
	 * 
	 * @return
	 */
	public boolean isValid() {
		return valid;
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
		final float[] values = new float[boundingBox.size()];
		for(int i = 0; i < boundingBox.size(); i++) {
			values[i] = boundingBox.get(i);
		}
		
		return SSTableHelper.floatArrayToIEEE754ByteBuffer(values).array();
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
	 * Tests if two bounding boxes share some space
	 * 
	 * For each dimension:
	 * 
	 * Case 1: 1 overlaps 2 at the left end
	 *  |--------|                      // 1
	 *      |------------|              // 2
	 *
	 * Case 2: 1 overlaps 2 at the tight end
	 *            |--------|            // 1
	 *   |------------|                 // 2
	 *
	 * Case 3: 1 is inside 2
	 *    |-------------------|         // 1
	 *  |-----------------------|       // 2
	 *
	 * Case 4: 2 is inside 1
	 * |-----------------------|        // 1
	 *      |----------|                // 2
	 *
	 * Case 5: 1 = 2
	 *            |--------|            // 1
	 *            |--------|            // 2
	 * 
	 * Case 6: No overlapping
	 * |-------|                        // 1
	 *               |---------|        // 2
	 * @param boundingBox
	 * @return
	 */
	public boolean overlaps(final BoundingBox boundingBox) {
		
		// Null does overlap with nothing
		if(boundingBox == null) {
			return false;
		}
		
		// Both boxes are equal (Case 5)
		if(equals(boundingBox)) {
			return true;
		}
		
		// Dimensions are not equal
		if(boundingBox.getDimension() != getDimension()) {
			return false;
		}
		
		// Check the overlapping in each dimension d
		for(int d = 0; d < getDimension(); d++) {
			
			// Case 1 or 3
			if(isCoveringPointInDimension(boundingBox.getCoordinateLow(d), d)) {
				continue;
			}
			
			// Case 2 or 3
			if(isCoveringPointInDimension(boundingBox.getCoordinateHigh(d), d)) {
				continue;
			}
			
			// Case 4 
			if(boundingBox.isCoveringPointInDimension(getCoordinateLow(d), d)) {
				continue;
			}
			
			// None of the above conditions matches (Case 6)
			return false;
		}
		
		return true;
	}
	
	/**
	 * Does the bounding box covers the point in the dimension?
	 * @param point
	 * @param dimension
	 * @return
	 */
	public boolean isCoveringPointInDimension(float point, int dimension) {
		if(getCoordinateLow(dimension) <= point && getCoordinateHigh(dimension) >= point) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Get the extent for the dimension
	 * @param dimension
	 * @return
	 */
	public float getExtent(final int dimension) {
		return boundingBox.get((2 * dimension) + 1);
	}
	
	/**
	 * The the lowest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public float getCoordinateLow(final int dimension) {
		return boundingBox.get(2 * dimension);
	}
	
	/**
	 * The the highest coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public float getCoordinateHigh(final int dimension) {
		return getCoordinateLow(dimension) + getExtent(dimension);
	}
	
	/**
	 * Return the dimension of the bounding box
	 * @return
	 */
	public int getDimension() {
		
		if(! valid) {
			return INVALID_DIMENSION;
		}
		
		return boundingBox.size() / 2;
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
	 * Compare to an other object
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
	
}
