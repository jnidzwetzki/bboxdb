/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.misc.Const;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHeaderParser;
import org.junit.Assert;
import org.junit.Test;


public class TestRoutingHeader {

	/**
	 * Test the hop parser
	 */
	@Test
	public void testRoutingHeaderHopParser1() {
		final RoutingHeader routingHeader = new RoutingHeader();
		routingHeader.setRoutingList(new ArrayList<DistributedInstance>());

		// Get routing list as string and parse list
		final String stringRoutingList = routingHeader.getRoutingListAsString();
		routingHeader.setRoutingList(stringRoutingList);
		
		final List<DistributedInstance> parsedRoutingList = routingHeader.getRoutingList();
		Assert.assertEquals(0, parsedRoutingList.size());
	}
	
	/**
	 * Test the hop parser
	 */
	@Test
	public void testRoutingHeaderHopParser2() {
		final DistributedInstance instance1 = new DistributedInstance("host1:123");
		final DistributedInstance instance2 = new DistributedInstance("host2:345");
		final DistributedInstance instance3 = new DistributedInstance("host3:567");
		
		final ArrayList<DistributedInstance> routingList = new ArrayList<DistributedInstance>();
		routingList.add(instance1);
		routingList.add(instance2);
		routingList.add(instance3);
		
		final RoutingHeader routingHeader = new RoutingHeader();
		routingHeader.setRoutingList(routingList);
		
		// Get routing list as string and parse list
		final String stringRoutingList = routingHeader.getRoutingListAsString();
		routingHeader.setRoutingList(stringRoutingList);
		
		final List<DistributedInstance> parsedRoutingList = routingHeader.getRoutingList();
		Assert.assertEquals(3, parsedRoutingList.size());
		Assert.assertTrue(parsedRoutingList.contains(instance1));
		Assert.assertTrue(parsedRoutingList.contains(instance2));
		Assert.assertTrue(parsedRoutingList.contains(instance3));
	}
	
	/**
	 * Test the encoding and the decoding of an unrouted package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testUnroutedPackageHeader() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(false);
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteArrayInputStream bis = new ByteArrayInputStream(encodedBytes);
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bis);
		
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test the encoding and the decoding of an routed package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testRoutedPackageHeader1() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "node1:12,node2:23");
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteArrayInputStream bis = new ByteArrayInputStream(encodedBytes);
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bis);
		
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test the encoding and the decoding of an routed package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testRoutedPackageHeader2() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "");
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteArrayInputStream bis = new ByteArrayInputStream(encodedBytes);
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bis);
		
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test the encoding and the decoding of an routed package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testRoutedPackageHeader3() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "node1:12,node2:23");
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		final ByteBuffer bb = ByteBuffer.wrap(encodedBytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		Assert.assertEquals(0, bb.position());
		
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bb);
		
		Assert.assertEquals(0, bb.remaining());
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test the encoding and the decoding of an routed package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testRoutedPackageHeader4() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "");
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		final ByteBuffer bb = ByteBuffer.wrap(encodedBytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		Assert.assertEquals(0, bb.position());
		
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bb);
		
		Assert.assertEquals(0, bb.remaining());
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test set valid next hops
	 */
	@Test
	public void testSetHopValid() {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "node1:12,node2:23");
		routingHeader.setHop((short) 0);
		routingHeader.setHop((short) 1);
	}
	
	/**
	 * Test set invalid next hops
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testSetHopInvalid() {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "node1:12,node2:23");
		routingHeader.setHop((short) 2);
	}
	
	/**
	 * Test header dispatch
	 */
	@Test
	public void testDispatchHeader() {
		final DistributedInstance hop1 = new DistributedInstance("node1:12");
		final DistributedInstance hop2 = new DistributedInstance("node2:23");
		
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 0, Arrays.asList(new DistributedInstance[] {hop1, hop2} ));
		Assert.assertEquals(0, routingHeader.getHop());
		Assert.assertFalse(routingHeader.reachedFinalInstance());
		Assert.assertEquals(hop1, routingHeader.getHopInstance());
		
		final boolean res1 = routingHeader.dispatchToNextHop();
		Assert.assertTrue(res1);
		Assert.assertTrue(routingHeader.reachedFinalInstance());
		Assert.assertEquals(1, routingHeader.getHop());
		Assert.assertEquals(hop2, routingHeader.getHopInstance());
		
		final boolean res2 = routingHeader.dispatchToNextHop();
		Assert.assertFalse(res2);
		Assert.assertTrue(routingHeader.reachedFinalInstance());
		Assert.assertEquals(1, routingHeader.getHop());
		
		final boolean res3 = routingHeader.dispatchToNextHop();
		Assert.assertFalse(res3);
		Assert.assertTrue(routingHeader.reachedFinalInstance());
	}
	
}
