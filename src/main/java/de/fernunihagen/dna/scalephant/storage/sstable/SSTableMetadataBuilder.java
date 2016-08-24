package de.fernunihagen.dna.scalephant.storage.sstable;

import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.SStableMetaData;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class SSTableMetadataBuilder {
	
	/**
	 * The amount of tupes
	 */
	protected long tuples = 0;
	
	/**
	 * The timestamp of the oldest tuple
	 */
	protected long oldestTuple = Long.MAX_VALUE;
	
	/**
	 * The timestamp of the newest tuple
	 */
	protected long newestTuple = Long.MIN_VALUE;

	/**
	 * The coresponding bounding box
	 */
	protected BoundingBox boundingBox;
	
	/**
	 * Update the metadata 
	 */
	public void addTuple(final Tuple tuple) {
		tuples++;
		
		if(boundingBox == null) {
			boundingBox = tuple.getBoundingBox();
		} else {
			// Calculate the bounding box of the current bounding box and
			// the bounding box of the tuple
			boundingBox = BoundingBox.getBoundingBox(boundingBox, tuple.getBoundingBox());
		}
				
		// Update the newest and the oldest tuple
		newestTuple = Math.max(newestTuple, tuple.getTimestamp());
		oldestTuple = Math.min(oldestTuple, tuple.getTimestamp());
	}
	
	/**
	 * Get the metadata object for the seen tuples
	 * @return
	 */
	public SStableMetaData getMetaData() {
		float[] boundingBoxArray = {};
		
		if(boundingBox != null) {
			boundingBoxArray = boundingBox.toFloatArray();
		}
		
		return new SStableMetaData(tuples, oldestTuple, newestTuple, boundingBoxArray);
	}
}
