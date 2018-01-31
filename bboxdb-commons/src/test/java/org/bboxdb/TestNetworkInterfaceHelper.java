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

import java.net.Inet4Address;
import java.net.SocketException;
import java.util.List;

import org.bboxdb.commons.NetworkInterfaceHelper;
import org.junit.Assert;
import org.junit.Test;


public class TestNetworkInterfaceHelper {

	/**
	 * Get the local IPs
	 * @throws SocketException 
	 */
	@Test
	public void testGetLocalIps() throws SocketException {
		final List<Inet4Address> localIps = NetworkInterfaceHelper.getNonLoopbackIPv4();
		Assert.assertTrue(localIps.size() > 0);
	}
	
	/**
	 * Get the first IP as string
	 * @throws SocketException 
	 */
	@Test
	public void testGetFirstIpAsString() throws SocketException {
		final String firstIp = NetworkInterfaceHelper.getFirstNonLoopbackIPv4AsString();
		Assert.assertTrue(firstIp.length() > 5);
	}

}
