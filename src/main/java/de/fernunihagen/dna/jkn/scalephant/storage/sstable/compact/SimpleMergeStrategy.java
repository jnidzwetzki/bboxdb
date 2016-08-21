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
	 * The number of tables to merge per task
	 */
	protected final static int MAX_MERGE_TABLES_PER_JOB = 100;
	
	/**
	 * Number of tables that will trigger a big compactification
	 */
	protected final static int BIG_TABLE_THRESHOLD = 20;

	@Override
	public MergeTask getMergeTask(final List<SSTableFacade> sstables) {
		
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
		
		final List<SSTableFacade> bigCompacts = new ArrayList<SSTableFacade>();
		for(SSTableFacade ssTableFacade : sstables) {
			bigCompacts.add(ssTableFacade);
			
			if(bigCompacts.size() >= MAX_MERGE_TABLES_PER_JOB) {
				break;
			}
		}

		final MergeTask mergeTask = new MergeTask();
		mergeTask.setMajorCompactTables(bigCompacts);
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
				
				if(smallCompacts.size() >= MAX_MERGE_TABLES_PER_JOB) {
					break;
				}
			}
		}
		
		// Create compact task
		final MergeTask mergeTask = new MergeTask();
		mergeTask.setMinorCompactTables(smallCompacts);
		return mergeTask;
	}
	
	@Override
	public long getCompactorDelay() {
		return SSTableConst.COMPACT_THREAD_DELAY;
	}

}
