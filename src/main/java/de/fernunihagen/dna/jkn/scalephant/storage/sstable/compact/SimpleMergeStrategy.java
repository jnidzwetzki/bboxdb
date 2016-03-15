package de.fernunihagen.dna.jkn.scalephant.storage.sstable.compact;

import java.util.ArrayList;
import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableConst;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;

public class SimpleMergeStrategy implements MergeStragegy {
	
	/**
	 * Merge small tables into a big one
	 */
	protected final static int SMALL_TABLE = 10000;

	@Override
	public MergeTask getMergeTasks(final List<SSTableFacade> sstables) {
		
		final List<SSTableFacade> smallCompacts = new ArrayList<SSTableFacade>();
		
		for(final SSTableFacade facade : sstables) {
			if(facade.getSsTableMetadata().getTuples() < SMALL_TABLE) {
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
