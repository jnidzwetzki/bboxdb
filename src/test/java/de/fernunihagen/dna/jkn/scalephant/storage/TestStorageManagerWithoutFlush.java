package de.fernunihagen.dna.jkn.scalephant.storage;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;

/**
 * Run the same test as the normal storage manager. The difference is, that
 * all data is kept in memory. The flush test is disabled. Therefore, the 
 * storage manager is forced to scan a lot of unflushed memtables.
 *
 */
public class TestStorageManagerWithoutFlush extends TestStorageManager {
	
	/**
	 * The amount of tuples for the big insert test
	 */
	protected final static int BIG_INSERT_TUPLES = 100000;

	@BeforeClass
	public static void changeConfigToMemory() {
		ScalephantConfigurationManager.getConfiguration().setStorageRunMemtableFlushThread(false);
	}
	
	@AfterClass
	public static void changeConfigToPersistent() {
		ScalephantConfigurationManager.getConfiguration().setStorageRunMemtableFlushThread(true);
	}
}
