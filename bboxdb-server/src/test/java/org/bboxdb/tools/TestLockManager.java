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
package org.bboxdb.tools;

import java.util.List;

import org.bboxdb.network.server.connection.lock.LockEntry;
import org.bboxdb.network.server.connection.lock.LockManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLockManager {

	/**
	 * Lock object 1
	 */
	private final static Object LOCK_OBJECT_1 = new Object();
	
	/**
	 * Lock object 2
	 */
	private final static Object LOCK_OBJECT_2 = new Object();

	/**
	 * The lock manager
	 */
	private LockManager lockManager;
	
	@Before
	public void before() {
		this.lockManager = new LockManager();
	}
	
	@Test(timeout=60000)
	public void testLockManager1() {
		final boolean result = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result);
	}
	
	@Test(timeout=60000)
	public void testLockManager2() {
		final boolean result1 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result1);
		
		final boolean result2 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertTrue(result2);
		
		final boolean result3 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 222, false);
		Assert.assertFalse(result3);
	}
	
	@Test(timeout=60000)
	public void testLockManager3() {
		final boolean result1 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result1);
		
		final boolean result2 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertFalse(result2);
		
		final boolean result3 = lockManager.lockTuple(LOCK_OBJECT_2, (short) 1, "abc", "1234", 12, false);
		Assert.assertFalse(result3);
	}
	
	@Test(timeout=60000)
	public void testLockManager4() {
		final boolean result1 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result1);
		
		final boolean result2 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertTrue(result2);
		
		Assert.assertEquals(2, lockManager.getAllLocksForObject(LOCK_OBJECT_1).size());
		Assert.assertEquals(0, lockManager.getAllLocksForObject(LOCK_OBJECT_2).size());

		lockManager.removeAllLocksForObject(LOCK_OBJECT_1);
		
		final boolean result3 = lockManager.lockTuple(LOCK_OBJECT_2, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result3);	
		
		final boolean result4 = lockManager.lockTuple(LOCK_OBJECT_2, (short) 1, "def", "1234", 12, false);
		Assert.assertTrue(result4);
		
		Assert.assertEquals(0, lockManager.getAllLocksForObject(LOCK_OBJECT_1).size());
		Assert.assertEquals(2, lockManager.getAllLocksForObject(LOCK_OBJECT_2).size());
	}
	
	@Test(timeout=60000)
	public void testLockManager5() {
		final boolean result1 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result1);
		
		final boolean result2 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertTrue(result2);
		
		lockManager.removeLockForConnectionAndKey(LOCK_OBJECT_1, "abc", "1234");
		lockManager.removeLockForConnectionAndKey(LOCK_OBJECT_1, "def", "1234");
		
		final boolean result3 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result3);
		
		final boolean result4 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertTrue(result4);
	}
	
	@Test(timeout=60000)
	public void testLockManager6() {
		final boolean result1 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result1);
		
		final boolean result2 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertTrue(result2);
		
		lockManager.removeLockForConnectionAndKey(LOCK_OBJECT_2, "abc", "1234");
		lockManager.removeLockForConnectionAndKey(LOCK_OBJECT_2, "def", "1234");
		
		final boolean result3 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertFalse(result3);
		
		final boolean result4 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertFalse(result4);
	}
	
	@Test(timeout=60000)
	public void testLockManager7() {
		final boolean result1 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result1);
		
		final boolean result2 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertTrue(result2);
		
		lockManager.removeLockForConnectionAndKey(LOCK_OBJECT_1, "abc", "4567");
		lockManager.removeLockForConnectionAndKey(LOCK_OBJECT_1, "def", "4567");
		
		final boolean result3 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertFalse(result3);
		
		final boolean result4 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "def", "1234", 12, false);
		Assert.assertFalse(result4);
	}
	
	@Test(timeout=60000)
	public void testLockManager8() {
		final boolean result1 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 1, "abc", "1234", 12, false);
		Assert.assertTrue(result1);
		
		final boolean result2 = lockManager.lockTuple(LOCK_OBJECT_1, (short) 2, "def", "5678", 12, false);
		Assert.assertTrue(result2);
		
		final List<LockEntry> removeResult1 
			= lockManager.removeAllForLocksForObjectAndSequence(LOCK_OBJECT_1, (short) 2);
		
		Assert.assertFalse(removeResult1.isEmpty());
		Assert.assertEquals("def", removeResult1.get(0).getTable());
		Assert.assertEquals("5678", removeResult1.get(0).getKey());
		
		final List<LockEntry> removeResult2 
			= lockManager.removeAllForLocksForObjectAndSequence(LOCK_OBJECT_1, (short) 2);
	
		Assert.assertTrue(removeResult2.isEmpty());
	}
}
