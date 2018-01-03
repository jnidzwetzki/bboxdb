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
package org.bboxdb;

import java.util.concurrent.Callable;

import org.bboxdb.commons.Retryer;
import org.junit.Assert;
import org.junit.Test;

public class TestRetyer {

	@Test
	public void testSuccess() throws InterruptedException {
		final Callable<Boolean> callable = new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				return true;
			}
		};
		
		final Retryer<Boolean> retryer = new Retryer<>(10, 100, callable);
		
		Assert.assertFalse(retryer.isDone());
		Assert.assertFalse(retryer.isSuccessfully());
		Assert.assertTrue(retryer.getResult() == null);
		Assert.assertEquals(0, retryer.getNeededExecutions());
		Assert.assertEquals(10, retryer.getMaxExecutions());

		final boolean result = retryer.execute();
		Assert.assertTrue(result);
		Assert.assertTrue(retryer.getResult());
		Assert.assertEquals(1, retryer.getNeededExecutions());
	}
	
	@Test
	public void testSuccessAfter5() throws InterruptedException {
		final Callable<Boolean> callable = new Callable<Boolean>() {

			protected int execution = 0;
			
			@Override
			public Boolean call() throws Exception {
								
				execution++;
				
				if(execution <= 5) {
					throw new Exception("Error");
				}
				
				return true;
			}
		};
		
		final Retryer<Boolean> retryer = new Retryer<>(10, 100, callable);
		
		Assert.assertFalse(retryer.isDone());
		Assert.assertFalse(retryer.isSuccessfully());
		Assert.assertTrue(retryer.getResult() == null);
		Assert.assertEquals(0, retryer.getNeededExecutions());
		Assert.assertEquals(10, retryer.getMaxExecutions());

		final boolean result = retryer.execute();
		Assert.assertTrue(result);
		Assert.assertTrue(retryer.getResult());
		Assert.assertEquals(6, retryer.getNeededExecutions());
		Assert.assertTrue(retryer.getLastException() != null);
	}

	@Test
	public void testSuccessAfter20() throws InterruptedException {
		final Callable<Boolean> callable = new Callable<Boolean>() {

			protected int execution = 0;
			
			@Override
			public Boolean call() throws Exception {
								
				execution++;
				
				if(execution < 20) {
					throw new Exception("Error");
				}
				
				return true;
			}
		};
		
		final Retryer<Boolean> retryer = new Retryer<>(10, 100, callable);
		
		Assert.assertFalse(retryer.isDone());
		Assert.assertFalse(retryer.isSuccessfully());
		Assert.assertTrue(retryer.getResult() == null);
		Assert.assertEquals(0, retryer.getNeededExecutions());
		Assert.assertEquals(10, retryer.getMaxExecutions());

		final boolean result = retryer.execute();
		Assert.assertFalse(result);
		Assert.assertTrue(retryer.getResult() == null);
		Assert.assertEquals(10, retryer.getNeededExecutions());
		Assert.assertTrue(retryer.getLastException() != null);
	}


}
