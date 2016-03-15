package de.fernunihagen.dna.jkn.scalephant.storage.sstable.compact;

import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;

public interface MergeStragegy {

	/**
	 * Comculate the merge tasks
	 * @param sstables
	 */
	public abstract MergeTask getMergeTasks(final List<SSTableFacade> sstables);

	/**
	 * Get the delay for the compact thread
	 * @return
	 */
	public abstract int getCompactorDelay();

}