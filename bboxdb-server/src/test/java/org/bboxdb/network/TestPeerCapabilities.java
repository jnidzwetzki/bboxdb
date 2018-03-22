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

import org.bboxdb.network.capabilities.PeerCapabilities;
import org.junit.Assert;
import org.junit.Test;

public class TestPeerCapabilities {

	/**
	 * Test the peer capabilities
	 */
	@Test(timeout=60000)
	public void testPeerCapabilities1() {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		Assert.assertTrue(peerCapabilities.toString().length() > 10);
	}
	
	/**
	 * Test the peer capabilities
	 */
	@Test(timeout=60000)
	public void testPeerCapabilities2() {
		final PeerCapabilities peerCapabilities1 = new PeerCapabilities();
		final PeerCapabilities peerCapabilities2 = new PeerCapabilities();
		Assert.assertTrue(peerCapabilities1.equals(peerCapabilities2));
		Assert.assertEquals(peerCapabilities1.hashCode(), peerCapabilities2.hashCode());
	}
	
	/**
	 * Test the peer capabilities
	 */
	@Test(expected=IllegalStateException.class)
	public void testPeerCapabilitiesRO1() {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		peerCapabilities.freeze();
		peerCapabilities.setGZipCompression();
	}
	
	/**
	 * Test the peer capabilities
	 */
	@Test(expected=IllegalStateException.class)
	public void testPeerCapabilitiesRO2() {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		peerCapabilities.freeze();
		peerCapabilities.clearGZipCompression();
	}
	
	/**
	 * Test creation with empty bytes
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testPeerCapabilitiesConstruct() {
		new PeerCapabilities(new byte[20]);
	}
}
