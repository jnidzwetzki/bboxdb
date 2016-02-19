package de.fernunihagen.dna.jkn.scalephant.storage;

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
	 * Tests if two bounding boxes share some space
	 * @param boundingBox
	 * @return
	 */
	public boolean overlaps(final BoundingBox boundingBox) {
		
		// TODO: Implement
		return false;
	}
	
	/**
	 * The the coordinate for the dimension
	 * @param dimension
	 * @return
	 */
	public float getCoordinate(final int dimension) {
		return boundingBox.get(dimension);
	}
	
	/**
	 * Get the extent for the dimension
	 * @param dimension
	 * @return
	 */
	public float getExtent(final int dimension) {
		return boundingBox.get(dimension + 1);
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
		return "BoundingBox [boundingBox=" + boundingBox + "]";
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
