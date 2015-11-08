package de.fernunihagen.dna.jkn.scalephant.storage;

import java.util.List;

public class BoundingBox {
	
	/**
	 * The variable boundingBox contains a bounding box for a tuple
	 * the bunding box for n dimensions is structured as follows:
	 * 
	 * boundingBox[0] = x_1
	 * boundingBox[1] = y_1
	 * [...]
	 * boundingBox[n-1] = x_n
	 * boundingBox[n] = y_n
	 */
	protected List<Short> boundingBox;
	
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
	
}
