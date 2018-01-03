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

import org.bboxdb.storage.sstable.reader.SSTableFacade;

public class MergeTask {

	/**
	 * The tables that should be compacted by a minor compact
	 * @return
	 */
	protected List<SSTableFacade> compactTables = new ArrayList<>();

	public MergeTaskType taskType = MergeTaskType.UNKNOWN;
	
	public List<SSTableFacade> getCompactTables() {
		return compactTables;
	}

	public void setCompactTables(final List<SSTableFacade> minorCompactTables) {
		this.compactTables = minorCompactTables;
	}
	
	public MergeTaskType getTaskType() {
		return taskType;
	}
	
	public void setTaskType(final MergeTaskType taskType) {
		this.taskType = taskType;
	}

	@Override
	public String toString() {
		return "MergeTask [compactTables=" + compactTables + ", taskType=" + taskType + "]";
	}
}
