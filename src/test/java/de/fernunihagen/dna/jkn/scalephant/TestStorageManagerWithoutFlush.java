package de.fernunihagen.dna.jkn.scalephant;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;

/**
 * Run the same test as the normal storage manager. The difference is, that
 * all data is kept in memory. The flush test is disabled. Therefore, the 
 * storage manager is forced to scan a lot of unflushed memtables.
 *
 */
public class TestStorageManagerWithoutFlush extends TestStorageManager {

	@BeforeClass
	public static void changeConfigToMemory() {
		StorageInterface.getStorageConfiguration().setRunMemtableFlushThread(false);
	}
	
	@AfterClass
	public static void changeConfigToPersistent() {
		StorageInterface.getStorageConfiguration().setRunMemtableFlushThread(true);
	}
}
