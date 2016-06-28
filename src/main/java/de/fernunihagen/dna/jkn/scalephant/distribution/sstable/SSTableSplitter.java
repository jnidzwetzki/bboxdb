package de.fernunihagen.dna.jkn.scalephant.distribution.sstable;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;

public interface SSTableSplitter {
	
	/**
	 * Is a split needed?
	 * @param totalTuplesInTable
	 * @return
	 */
	public boolean isSplitNeeded(final int totalTuplesInTable);

	/**
	 * Perform a SSTable split
	 * @param ssTableName
	 */
	public void performSplit(final SSTableName ssTableName);
	
}
