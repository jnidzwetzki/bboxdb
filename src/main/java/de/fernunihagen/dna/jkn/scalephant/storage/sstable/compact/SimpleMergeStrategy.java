package de.fernunihagen.dna.jkn.scalephant.storage.sstable.compact;

import java.util.ArrayList;
import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableConst;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;

public class SimpleMergeStrategy implements MergeStrategy {
	
	/**
	 * Merge small tables into a big one
	 */
	protected final static int SMALL_TABLE_THRESHOLD = 10000;
	
	/**
	 * Number of tables that will trigger a big compactification
	 */
	protected final static int BIG_TABLE_THRESHOLD = 20;

	@Override
	public MergeTask getMergeTasks(final List<SSTableFacade> sstables) {
		
		if(sstables.size() > BIG_TABLE_THRESHOLD) {
			return generateBigCompactTask(sstables);
		}
		
		return generateSmallCompactTask(sstables);
	}

	/**
	 * Generate a big compact task
	 * @param sstables
	 * @return
	 */
	protected MergeTask generateBigCompactTask(
			final List<SSTableFacade> sstables) {
		
		final MergeTask mergeTask = new MergeTask();
		mergeTask.setMajorCompactTables(sstables);
		return mergeTask;
	}

	/**
	 * Generate a small compact task
	 * @param sstables
	 * @return
	 */
	protected MergeTask generateSmallCompactTask(
			final List<SSTableFacade> sstables) {
		final List<SSTableFacade> smallCompacts = new ArrayList<SSTableFacade>();
		
		for(final SSTableFacade facade : sstables) {
			if(facade.getSsTableMetadata().getTuples() < SMALL_TABLE_THRESHOLD) {
				smallCompacts.add(facade);
			}
		}
		
		// Create compact task
		final MergeTask mergeTask = new MergeTask();
		mergeTask.setMinorCompactTables(smallCompacts);
		return mergeTask;
	}
	
	@Override
	public int getCompactorDelay() {
		return SSTableConst.COMPACT_THREAD_DELAY;
	}

}
