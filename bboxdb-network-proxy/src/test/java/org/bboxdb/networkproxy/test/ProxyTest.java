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

import java.io.IOException;
import java.net.UnknownHostException;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.distribution.membership.MembershipConnectionService;
import org.bboxdb.networkproxy.ProxyConst;
import org.bboxdb.networkproxy.ProxyMain;
import org.bboxdb.networkproxy.client.NetworkProxyClient;
import org.bboxdb.storage.util.EnvironmentHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProxyTest {
	
	/**
	 * The BBoxDB Main
	 */
	private static BBoxDBMain bboxDBMain;
	
	/**
	 * The proxy class
	 */
	private ProxyMain proxyMain;
	
	@BeforeClass
	public static void init() throws Exception {
		
		// Cleanup Zookeeper
		EnvironmentHelper.resetTestEnvironment();
		
		bboxDBMain = new BBoxDBMain();
		bboxDBMain.init();
		bboxDBMain.start();
		
		// Allow connections to localhost (needed for travis CI test)
		MembershipConnectionService.getInstance().clearBlacklist();

		Thread.currentThread();
		
		// Wait some time to let the server process start
		Thread.sleep(5000);
	}

	@AfterClass
	public static void shutdown() throws Exception {
		if(bboxDBMain != null) {
			bboxDBMain.stop();
			bboxDBMain = null;
		}

		// Wait some time for socket re-use
		Thread.sleep(5000);
	}

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

	@Test(timeout=60000)
	public void testDisconnect() throws UnknownHostException, IOException {
		final NetworkProxyClient networkProxyClient = new NetworkProxyClient("localhost", ProxyConst.PROXY_PORT);
		networkProxyClient.close();
	}
}
