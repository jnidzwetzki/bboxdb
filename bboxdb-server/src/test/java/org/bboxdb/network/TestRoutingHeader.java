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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.misc.Const;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHeaderParser;
import org.bboxdb.network.routing.RoutingHop;
import org.junit.Assert;
import org.junit.Test;


public class TestRoutingHeader {

	/**
	 * Test the hop parser
	 */
	@Test
	public void testRoutingHeaderHopParser1() {
		final RoutingHeader routingHeader = new RoutingHeader((short) 0, new ArrayList<RoutingHop>());

		// Get routing list as string and parse list
		final String stringRoutingList = routingHeader.getRoutingListAsString();
		routingHeader.setRoutingList(stringRoutingList);
		
		final List<RoutingHop> parsedRoutingList = routingHeader.getRoutingList();
		Assert.assertEquals(0, parsedRoutingList.size());
	}
	
	/**
	 * Test the hop parser
	 */
	@Test
	public void testRoutingHeaderHopParser2() {
		final RoutingHop hop1 = new RoutingHop(new BBoxDBInstance("host1:50500"), Arrays.asList(123l));
		final RoutingHop hop2 = new RoutingHop(new BBoxDBInstance("host2:50500"), Arrays.asList(456l));
		final RoutingHop hop3 = new RoutingHop(new BBoxDBInstance("host3:50500"), Arrays.asList(789l));
		
		final List<RoutingHop> routingList = new ArrayList<>();
		routingList.add(hop1);
		routingList.add(hop2);
		routingList.add(hop3);
		
		final RoutingHeader routingHeader = new RoutingHeader((short) 0, routingList);
		
		// Get routing list as string and parse list
		final String stringRoutingList = routingHeader.getRoutingListAsString();
		routingHeader.setRoutingList(stringRoutingList);
		
		final List<RoutingHop> parsedRoutingList = routingHeader.getRoutingList();
		Assert.assertEquals(3, parsedRoutingList.size());
		Assert.assertTrue(parsedRoutingList.contains(hop1));
		Assert.assertTrue(parsedRoutingList.contains(hop2));
		Assert.assertTrue(parsedRoutingList.contains(hop3));
	}
	
	/**
	 * Test the routing hop parser
	 */
	@Test
	public void testRoutingHopParser3() {
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "node1:12,1;node2:23,2");
		Assert.assertEquals(2, routingHeader.getRoutingList().size());
		Assert.assertEquals(1, routingHeader.getRoutingList().get(0).getDistributionRegions().size());
		Assert.assertEquals(1, routingHeader.getRoutingList().get(1).getDistributionRegions().size());
		Assert.assertTrue(routingHeader.getRoutingList().get(0).getDistributionRegions().contains(1l));
		Assert.assertTrue(routingHeader.getRoutingList().get(1).getDistributionRegions().contains(2l));
	}
	
	/**
	 * Test the routing hop parser
	 */
	@Test
	public void testRoutingHopParser4() {
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "node1:12,1,2,3;node2:23,2");
		Assert.assertEquals(2, routingHeader.getRoutingList().size());
		Assert.assertEquals(3, routingHeader.getRoutingList().get(0).getDistributionRegions().size());
		Assert.assertEquals(1, routingHeader.getRoutingList().get(1).getDistributionRegions().size());
		Assert.assertTrue(routingHeader.getRoutingList().get(0).getDistributionRegions().contains(1l));
		Assert.assertTrue(routingHeader.getRoutingList().get(0).getDistributionRegions().contains(2l));
		Assert.assertTrue(routingHeader.getRoutingList().get(0).getDistributionRegions().contains(3l));
		Assert.assertTrue(routingHeader.getRoutingList().get(1).getDistributionRegions().contains(2l));
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
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "node1:12,1;node2:23,2");
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
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "");
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
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "node1:12,1;node2:23,2");
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
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "");
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
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "node1:12,2;node2:23,1");
		routingHeader.setHop((short) 0);
		routingHeader.setHop((short) 1);
	}
	
	/**
	 * Test set invalid next hops
	 */
	@Test(expected=IllegalArgumentException.class)
	public void testSetHopInvalid() {
		final RoutingHeader routingHeader = new RoutingHeader((short) 10, "node1:12,1;node2:23,2");
		routingHeader.setHop((short) 2);
	}
	
	/**
	 * Test header dispatch
	 */
	@Test
	public void testDispatchHeader() {
		final RoutingHop hop1 = new RoutingHop(new BBoxDBInstance("host1:50500"), Arrays.asList(123l));
		final RoutingHop hop2 = new RoutingHop(new BBoxDBInstance("host2:50500"), Arrays.asList(456l));
		
		final List<RoutingHop> routingList = Arrays.asList(new RoutingHop[] {hop1, hop2} );
		
		final RoutingHeader routingHeader = new RoutingHeader((short) 0, routingList);
		Assert.assertEquals(0, routingHeader.getHop());
		Assert.assertFalse(routingHeader.reachedFinalInstance());
		Assert.assertEquals(hop1, routingHeader.getRoutingHop());
		
		final boolean res1 = routingHeader.dispatchToNextHop();
		Assert.assertTrue(res1);
		Assert.assertTrue(routingHeader.reachedFinalInstance());
		Assert.assertEquals(1, routingHeader.getHop());
		Assert.assertEquals(hop2, routingHeader.getRoutingHop());
		
		final boolean res2 = routingHeader.dispatchToNextHop();
		Assert.assertFalse(res2);
		Assert.assertTrue(routingHeader.reachedFinalInstance());
		Assert.assertEquals(1, routingHeader.getHop());
		
		final boolean res3 = routingHeader.dispatchToNextHop();
		Assert.assertFalse(res3);
		Assert.assertTrue(routingHeader.reachedFinalInstance());
	}
	
}
