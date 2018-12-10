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
package org.bboxdb.networkproxy.test;

import org.bboxdb.networkproxy.ProxyMain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ProxyTest {
	
	/**
	 * The proxy class
	 */
	private ProxyMain proxyMain;

	@Before
	public synchronized void startProxyServer() {
		
		if(proxyMain != null) {
			throw new IllegalStateException("Proxy is already running");
		}
		
		proxyMain = new ProxyMain("127.0.0.1", "mycluster");
		proxyMain.run();
	}
	
	@After
	public synchronized void stopProxyServer() {
		proxyMain.close();
		proxyMain = null;
	}

	@Test
	public void testDisconnect() {
		
	}
}
