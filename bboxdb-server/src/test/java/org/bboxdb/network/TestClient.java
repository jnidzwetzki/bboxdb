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

import org.bboxdb.network.client.BBoxDBClient;
import org.junit.Test;
import org.mockito.Mockito;

public class TestClient {

	/**
	 * Test the settle requests method
	 */
	@Test(timeout=10000)
	public void testSettleRequests1() {
		final BBoxDBClient client = Mockito.spy(BBoxDBClient.class);
		client.settlePendingCalls(1000);
		Mockito.verify(client, Mockito.atLeast(1)).getInFlightCalls();
	}
	
	/**
	 * Test the settle requests method 2
	 */
	@Test(timeout=10000)
	public void testSettleRequests2() {
		final BBoxDBClient client = Mockito.spy(BBoxDBClient.class);
		Mockito.when(client.getInFlightCalls()).thenReturn(10);
		client.settlePendingCalls(1000);
		Mockito.verify(client, Mockito.atLeast(1)).getInFlightCalls();
	}
}
