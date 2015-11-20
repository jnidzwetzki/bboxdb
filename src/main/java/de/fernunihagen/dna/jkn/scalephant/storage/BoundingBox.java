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
	 * boundingBox[0] = x_1
	 * boundingBox[1] = y_1
	 * [...]
	 * boundingBox[n-1] = x_n
	 * boundingBox[n] = y_n
	 */
	protected final List<Long> boundingBox;
	
	public BoundingBox(Long... args) {
		boundingBox = new ArrayList<Long>(args.length);
		boundingBox.addAll(Arrays.asList(args));
	}
	
	public BoundingBox(long[] values) {
		boundingBox = new ArrayList<Long>(values.length);
		
		for(int i = 0; i < values.length; i++) {
			boundingBox.add(values[i]);
		}
	}
	
	/**
	 * Checks that the bounding box is valid
	 * 
	 * @return
	 */
	public boolean isValid() {
		return (boundingBox.size() / 2 == 0);
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
		final long[] values = new long[boundingBox.size()];
		for(int i = 0; i < boundingBox.size(); i++) {
			values[i] = boundingBox.get(i);
		}
		
		return SSTableHelper.longArrayToByteBuffer(values).array();
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
