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
package org.bboxdb.storage.wal;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.SSTableHelper;

public class WriteAheadLogManager {
	
	/**
	 * Get the WAL filename
	 * @param basedir
	 * @param memtableNumber
	 * @return
	 */
	public static File getFileForWal(final File basedir, final long memtableNumber) {
		return new File(basedir.getAbsolutePath() + "/" + "wal_" 
				+ memtableNumber + SSTableConst.MEMTABLE_WAL_SUFFIX);
	}
	
	/**
	 * Get all WAL files from the given basedir
	 * @param basedir
	 * @return
	 */
	public static List<File> getAllWalFiles(final File basedir) {
		final File[] files = basedir.listFiles((d, n) -> SSTableHelper.isFileNameWAL(n));
		return Arrays.asList(files);
	}
	
}
