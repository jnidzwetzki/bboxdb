/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package de.fernunihagen.dna.scalephant.storage.sstable.compact;

import java.util.ArrayList;
import java.util.List;

import de.fernunihagen.dna.scalephant.storage.sstable.SSTableConst;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;

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
	protected MergeTask generateBigCompactTask(final List<SSTableFacade> sstables) {
		
		final List<SSTableFacade> bigCompacts = new ArrayList<SSTableFacade>();
		for(SSTableFacade ssTableFacade : sstables) {
			bigCompacts.add(ssTableFacade);
			
			if(bigCompacts.size() >= MAX_MERGE_TABLES_PER_JOB) {
				break;
			}
		}

		final MergeTask mergeTask = new MergeTask();
		
		// One table can't be merged
		if(bigCompacts.size() > 1) {
			mergeTask.setMajorCompactTables(bigCompacts);
		}
		
		return mergeTask;
	}

	/**
	 * Generate a small compact task
	 * @param sstables
	 * @return
	 */
	protected MergeTask generateSmallCompactTask(final List<SSTableFacade> sstables) {
		
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
		
		// One table can't be merged
		if(smallCompacts.size() > 1) {
			mergeTask.setMinorCompactTables(smallCompacts);
		}
		
		return mergeTask;
	}
	
	@Override
	public long getCompactorDelay() {
		return SSTableConst.COMPACT_THREAD_DELAY;
	}

}
