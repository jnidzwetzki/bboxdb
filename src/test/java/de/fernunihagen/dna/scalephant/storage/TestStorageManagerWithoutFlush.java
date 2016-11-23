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
package de.fernunihagen.dna.scalephant.storage;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;

/**
 * Run the same test as the normal storage manager. The difference is, that
 * all data is kept in memory. The flush test is disabled. Therefore, the 
 * storage manager is forced to scan a lot of unflushed memtables.
 *
 */
public class TestStorageManagerWithoutFlush extends TestStorageManager {

	@BeforeClass
	public static void changeConfigToMemory() {
		ScalephantConfigurationManager.getConfiguration().setStorageRunMemtableFlushThread(false);
	}
	
	@AfterClass
	public static void changeConfigToPersistent() {
		ScalephantConfigurationManager.getConfiguration().setStorageRunMemtableFlushThread(true);
	}
	
	/**
	 * Number of tuples for big insert
	 * @return
	 */
	protected int getNumberOfTuplesForBigInsert() {
		return 10000;
	}
}
