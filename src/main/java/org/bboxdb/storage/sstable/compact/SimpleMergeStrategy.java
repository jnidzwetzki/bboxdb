/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.storage.sstable.compact;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.reader.SSTableFacade;

public class SimpleMergeStrategy implements MergeStrategy {
	
	/**
	 * Merge small tables into a big one
	 */
	protected final static int SMALL_TABLE_THRESHOLD = 10000;
	
	/**
	 * The number of tables to merge per task
	 */
	protected final static int MAX_MERGE_TABLES_PER_JOB = 10;
	
	/**
	 * Number of tables that will trigger a big compactification
	 */
	protected final static int BIG_TABLE_THRESHOLD = 5;
	
	/**
	 * The time a big table is excluded from compact tasks
	 */
	protected final static long BIG_TABLE_UNTOUCHED_TIME = TimeUnit.HOURS.toMillis(1);

	@Override
	public MergeTask getMergeTask(final List<SSTableFacade> sstables) {
		
		if(sstables.size() > BIG_TABLE_THRESHOLD) {
			return generateMajorCompactTask(sstables);
		}
		
		return generateMinorCompactTask(sstables);
	}

	/**
	 * Generate a major compact task
	 * @param sstables
	 * @return
	 */
	protected MergeTask generateMajorCompactTask(final List<SSTableFacade> sstables) {
		
		final long now = System.currentTimeMillis();
		
		// Any old big compact tables?
		final boolean bigCompactNeeded = sstables
			.stream()
			.filter(f -> f.getSsTableMetadata().getTuples() >= SMALL_TABLE_THRESHOLD)
			.anyMatch(f -> f.getSsTableReader().getLastModifiedTimestamp() + BIG_TABLE_UNTOUCHED_TIME < now);
		
		final MergeTask mergeTask = new MergeTask();
	
		// One table can't be merged
		if(bigCompactNeeded && sstables.size() > 1) {
			
			// In a major compact, all tables needs to be merged
			final List<SSTableFacade> bigCompacts = new ArrayList<>();
			bigCompacts.addAll(sstables);

			mergeTask.setMajorCompactTables(bigCompacts);
		}
		
		return mergeTask;
	}

	/**
	 * Generate a minor compact task
	 * @param sstables
	 * @return
	 */
	protected MergeTask generateMinorCompactTask(final List<SSTableFacade> sstables) {
		
		final List<SSTableFacade> smallCompacts = sstables
			.stream()
			.filter(f -> f.getSsTableMetadata().getTuples() < SMALL_TABLE_THRESHOLD)
			.limit(MAX_MERGE_TABLES_PER_JOB)
			.collect(Collectors.toList());

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
