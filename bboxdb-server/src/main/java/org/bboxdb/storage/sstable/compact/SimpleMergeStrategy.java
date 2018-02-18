/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
import java.util.stream.Collectors;

import org.bboxdb.misc.BBoxDBConfiguration;
import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.reader.SSTableFacade;

public class SimpleMergeStrategy implements MergeStrategy {
	
	/**
	 * The number of tables to merge per task
	 */
	protected final static int MAX_MERGE_TABLES_PER_JOB = 10;
	
	@Override
	public MergeTask getMergeTask(final List<SSTableFacade> sstables) {
		
		final MergeTask mergeTask = new MergeTask();
		final List<SSTableFacade> majorMergeTables = generateMajorCompactTask(sstables);
		
		if(majorMergeTables.size() > 1) {
			mergeTask.setTaskType(MergeTaskType.MAJOR);
			mergeTask.setCompactTables(majorMergeTables);
			return mergeTask;
		} 
		
		final List<SSTableFacade> minorMergeTables = generateMinorCompactTask(sstables);
		
		if(minorMergeTables.size() > 1) {
			
			if(minorMergeTables.size() == sstables.size()) {
				// All tables are included, handle as major compact
				mergeTask.setTaskType(MergeTaskType.MAJOR);
			} else {
				mergeTask.setTaskType(MergeTaskType.MINOR);
			}
			mergeTask.setCompactTables(minorMergeTables);
		}

		return mergeTask;
	}

	/**
	 * Generate a major compact task
	 * @param sstables
	 * @return
	 */
	protected List<SSTableFacade> generateMajorCompactTask(final List<SSTableFacade> sstables) {
		
		final long now = System.currentTimeMillis();
		
		final long smallTableThreshold = getSmallTableThreshold();
		
		// Any old big compact tables?
		final boolean bigCompactNeeded = sstables
			.stream()
			.filter(f -> f.getSsTableMetadata().getTuples() >= smallTableThreshold)
			.map(f -> f.getSsTableReader())
			.anyMatch(r -> r.getLastModifiedTimestamp() + SSTableConst.COMPACT_BIG_TABLE_UNTOUCHED_TIME < now);
			
		final List<SSTableFacade> bigCompacts = new ArrayList<>();
		
		// One table can't be merged
		if(bigCompactNeeded) {
			bigCompacts.addAll(sstables);
		}
		
		return bigCompacts;
	}

	/**
	 * The small table threshold
	 * @return
	 */
	protected long getSmallTableThreshold() {
		final BBoxDBConfiguration configuration = BBoxDBConfigurationManager.getConfiguration();
		return configuration.getMemtableEntriesMax() * 5;
	}

	/**
	 * Generate a minor compact task
	 * @param sstables
	 * @return
	 */
	protected List<SSTableFacade> generateMinorCompactTask(final List<SSTableFacade> sstables) {
		
		final long smallTableThreshold = getSmallTableThreshold();
		
		final List<SSTableFacade> smallCompacts = sstables
			.stream()
			.filter(f -> f.getSsTableMetadata().getTuples() < smallTableThreshold)
			.limit(MAX_MERGE_TABLES_PER_JOB)
			.collect(Collectors.toList());

		return smallCompacts;
	}
	
	@Override
	public long getCompactorDelay() {
		return SSTableConst.COMPACT_THREAD_DELAY;
	}

}
