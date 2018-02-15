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
package org.bboxdb.network;

import java.util.function.BiConsumer;

import org.bboxdb.network.client.NetworkOperationRetryer;
import org.bboxdb.network.client.future.OperationFuture;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.request.ListTablesRequest;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestNetworkOperationRetryer {

	/**
	 * The do nothing consumer
	 */
	protected BiConsumer<NetworkRequestPackage, OperationFuture> doNothingConsumer = (p, f) -> {};
	
	/**
	 * The empty package
	 */
	protected final NetworkRequestPackage emptyPackage = new ListTablesRequest((short) 12);
	
	/**
	 * The empty future
	 */
	protected final OperationFuture emptyFuture = null;
	
	@Test(expected=IllegalArgumentException.class)
	public void testPackageNotFound() {
		final NetworkOperationRetryer retryer 
			= new NetworkOperationRetryer(doNothingConsumer);
		
		retryer.handleFailure((short) 12, "");
		
		retryer.close();
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testPackageDuplicate() {
		final NetworkOperationRetryer retryer 
			= new NetworkOperationRetryer(doNothingConsumer);

		retryer.registerOperation((short) 12, emptyPackage, emptyFuture);
		retryer.registerOperation((short) 12, emptyPackage, emptyFuture);
		
		retryer.close();
	}
	
	@Test
	public void successOnFirst() {
		final NetworkOperationRetryer retryer 
			= new NetworkOperationRetryer(doNothingConsumer);
		retryer.registerOperation((short) 12, emptyPackage, emptyFuture);
		retryer.handleSuccess((short) 12);
		
		retryer.close();
	}

	@Test
	public void retry() throws InterruptedException {
		@SuppressWarnings("unchecked")
		final BiConsumer<NetworkRequestPackage, OperationFuture> consumer = Mockito.mock(BiConsumer.class);
		
		final NetworkOperationRetryer retryer 
			= new NetworkOperationRetryer(consumer);
		
		retryer.registerOperation((short) 12, emptyPackage, emptyFuture);
		
		final boolean result = retryer.handleFailure((short) 12, "");
		Assert.assertTrue(result);
		
		Thread.sleep(200);
		
		(Mockito.verify(consumer, Mockito.atLeastOnce())).accept(emptyPackage, emptyFuture);
		
		retryer.close();
	}
}
