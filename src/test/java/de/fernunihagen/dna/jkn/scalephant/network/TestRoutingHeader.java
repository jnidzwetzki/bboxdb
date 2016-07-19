package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.Const;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.DistributedInstance;
import de.fernunihagen.dna.jkn.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.jkn.scalephant.network.routing.RoutingHeaderParser;


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
	 */
	@Test
	public void testUnroutedPackageHeader() throws IOException {
		final RoutingHeader routingHeader = new RoutingHeader(false);
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteArrayInputStream bis = new ByteArrayInputStream(encodedBytes);
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bis);
		
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test the encoding and the decoding of an routed package
	 * @throws IOException 
	 */
	@Test
	public void testRoutedPackageHeader1() throws IOException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "node1:12,node2:23");
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteArrayInputStream bis = new ByteArrayInputStream(encodedBytes);
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bis);
		
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test the encoding and the decoding of an routed package
	 * @throws IOException 
	 */
	@Test
	public void testRoutedPackageHeader2() throws IOException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "");
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteArrayInputStream bis = new ByteArrayInputStream(encodedBytes);
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bis);
		
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
	
	/**
	 * Test the encoding and the decoding of an routed package
	 * @throws IOException 
	 */
	@Test
	public void testRoutedPackageHeader3() throws IOException {
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
	 */
	@Test
	public void testRoutedPackageHeader4() throws IOException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 10, "");
		final byte[] encodedBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		final ByteBuffer bb = ByteBuffer.wrap(encodedBytes);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		Assert.assertEquals(0, bb.position());
		
		final RoutingHeader resultRoutingHeader = RoutingHeaderParser.decodeRoutingHeader(bb);
		
		Assert.assertEquals(0, bb.remaining());
		Assert.assertEquals(routingHeader, resultRoutingHeader);
	}
}
