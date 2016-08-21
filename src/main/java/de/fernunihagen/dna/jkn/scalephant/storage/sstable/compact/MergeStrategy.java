package de.fernunihagen.dna.jkn.scalephant.storage.sstable.compact;

import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;

public interface MergeStrategy {

	/**
	 * Comculate the merge tasks
	 * @param sstables
	 */
	public abstract MergeTask getMergeTask(final List<SSTableFacade> sstables);

	/**
	 * Get the delay for the compact thread
	 * @return
	 */
	public abstract long getCompactorDelay();

}