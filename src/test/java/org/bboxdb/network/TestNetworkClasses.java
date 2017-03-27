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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.bboxdb.Const;
import org.bboxdb.distribution.membership.DistributedInstance;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.client.SequenceNumberGenerator;
import org.bboxdb.network.packages.NetworkPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CancelQueryRequest;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.DeleteTupleRequest;
import org.bboxdb.network.packages.request.HelloRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.request.ListTablesRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxRequest;
import org.bboxdb.network.packages.request.QueryBoundingBoxTimeRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryTimeRequest;
import org.bboxdb.network.packages.response.CompressionEnvelopeResponse;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.packages.response.ListTablesResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.packages.response.TupleResponse;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.util.MicroSecondTimestampProvider;
import org.junit.Assert;
import org.junit.Test;

public class TestNetworkClasses {
	
	/**
	 * The sequence number generator for our packages
	 */
	protected SequenceNumberGenerator sequenceNumberGenerator = new SequenceNumberGenerator();
	
	/**
	 * Convert a network package into a byte array
	 * @param networkPackage
	 * @param sequenceNumber
	 * @return
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	protected byte[] networkPackageToByte(final NetworkPackage networkPackage) 
			throws IOException, PackageEncodeException {
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		networkPackage.writeToOutputStream(bos);
		bos.flush();
		bos.close();
		return bos.toByteArray();
	}
	
	/**
	 * Ensure that all sequence numbers are distinct
	 */
	@Test
	public void testSequenceNumberGenerator1() {
		final int NUMBERS = 1000;
		final HashMap<Short, Short> sequenceNumberMap = new HashMap<Short, Short>();
		
		for(int i = 0; i < NUMBERS; i++) {
			final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
			Assert.assertFalse(sequenceNumberMap.containsKey(sequenceNumber));
			sequenceNumberMap.put(sequenceNumber, (short) 1);
		}
		
		Assert.assertEquals(sequenceNumberMap.size(), NUMBERS);
	}
	
	/**
	 * Ensure the generatror is able to create more than 2^16 numbers, even we have
	 * some overruns 
	 */
	@Test
	public void testSequenceNumberGenerator2() {
		final HashMap<Short, Short> sequenceNumberMap = new HashMap<Short, Short>();

		for(int i = 0; i < Integer.MAX_VALUE / 100; i++) {
			final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
			
			if(! sequenceNumberMap.containsKey(sequenceNumber)) {
				sequenceNumberMap.put(sequenceNumber, (short) 1);
			} else {
				short oldValue = sequenceNumberMap.get(sequenceNumber);
				sequenceNumberMap.put(sequenceNumber, (short) (oldValue + 1));
			}
		}
		
		Assert.assertEquals(65536, sequenceNumberMap.size());		
	}
	
	/**
	 * Test the encoding of the request package header
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testRequestPackageHeader() throws IOException, PackageEncodeException {
		final short currentSequenceNumber = sequenceNumberGenerator.getSequeneNumberWithoutIncrement();
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		
		final ListTablesRequest listTablesRequest = new ListTablesRequest(sequenceNumber);
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		listTablesRequest.writeToOutputStream(bos);
		bos.flush();
		bos.close();

		final byte[] encodedPackage = bos.toByteArray();
		
		Assert.assertEquals(18, encodedPackage.length);
		
		final ByteBuffer bb = ByteBuffer.wrap(encodedPackage);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		
		// Check fields
		Assert.assertEquals(currentSequenceNumber, bb.getShort());
		Assert.assertEquals(NetworkConst.REQUEST_TYPE_LIST_TABLES, bb.getShort());
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeInsertTuple1() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", BoundingBox.EMPTY_BOX, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, new SSTableName("test"), tuple);
		
		byte[] encodedVersion = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(bb);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(insertPackage.getRoutingHeader(), new RoutingHeader(false));
		Assert.assertEquals(insertPackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeInsertTuple2() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", new BoundingBox(1.3244343224, 232.232333343, 34324.343, 343243.0), "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, new SSTableName("test"), tuple);
		
		byte[] encodedVersion = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(bb);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(insertPackage.getRoutingHeader(), new RoutingHeader(false));
		Assert.assertEquals(insertPackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeInsertTupleWithCustomHeader() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 12, Arrays.asList(new DistributedInstance[] { new DistributedInstance("node1:3445")}));
		final Tuple tuple = new Tuple("key", BoundingBox.EMPTY_BOX, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, routingHeader, new SSTableName("test"), tuple);
		Assert.assertEquals(routingHeader, insertPackage.getRoutingHeader());
		
		byte[] encodedVersion = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(bb);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(routingHeader, decodedPackage.getRoutingHeader());
		Assert.assertFalse(insertPackage.getRoutingHeader().equals(new RoutingHeader(false)));
		Assert.assertEquals(insertPackage, decodedPackage);
	}
	
	
	/**
	 * The the encoding and decoding of an delete tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeDeleteTuple() throws IOException, PackageEncodeException {
		final long deletionTime = MicroSecondTimestampProvider.getNewTimestamp();
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final DeleteTupleRequest deletePackage = new DeleteTupleRequest(sequenceNumber, "test", "key", deletionTime);
		
		byte[] encodedVersion = networkPackageToByte(deletePackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final DeleteTupleRequest decodedPackage = DeleteTupleRequest.decodeTuple(bb);
				
		Assert.assertEquals(deletePackage.getKey(), decodedPackage.getKey());
		Assert.assertEquals(deletePackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(deletePackage.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(deletePackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of an create distribution group package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeCreateDistributionGroup() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final CreateDistributionGroupRequest groupPackage = new CreateDistributionGroupRequest(sequenceNumber, "test", (short) 3);
		
		byte[] encodedVersion = networkPackageToByte(groupPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final CreateDistributionGroupRequest decodedPackage = CreateDistributionGroupRequest.decodeTuple(bb);
				
		Assert.assertEquals(groupPackage.getDistributionGroup(), decodedPackage.getDistributionGroup());
		Assert.assertEquals(groupPackage.getReplicationFactor(), decodedPackage.getReplicationFactor());
		Assert.assertEquals(groupPackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of an create distribution group package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeDeleteDistributionGroup() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final DeleteDistributionGroupRequest groupPackage = new DeleteDistributionGroupRequest(sequenceNumber, "test");
		
		byte[] encodedVersion = networkPackageToByte(groupPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final DeleteDistributionGroupRequest decodedPackage = DeleteDistributionGroupRequest.decodeTuple(bb);
				
		Assert.assertEquals(groupPackage.getDistributionGroup(), decodedPackage.getDistributionGroup());
		Assert.assertEquals(groupPackage, decodedPackage);
	}
	
	/**
	 * The encoding and decoding of an next page package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeNextPage() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final short querySequence = 12;
		final NextPageRequest nextPageRequest = new NextPageRequest(sequenceNumber, querySequence);
		
		byte[] encodedVersion = networkPackageToByte(nextPageRequest);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final NextPageRequest decodedPackage = NextPageRequest.decodeTuple(bb);
				
		Assert.assertEquals(decodedPackage.getQuerySequence(), querySequence);
	}
	
	
	/**
	 * The  encoding and decoding of an cancel query package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeCancelQuery() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final short querySequence = 12;
		final CancelQueryRequest cancelQueryRequest = new CancelQueryRequest(sequenceNumber, querySequence);
		
		byte[] encodedVersion = networkPackageToByte(cancelQueryRequest);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final CancelQueryRequest decodedPackage = CancelQueryRequest.decodeTuple(bb);
				
		Assert.assertEquals(decodedPackage.getQuerySequence(), querySequence);
	}
	
	/**
	 * The the encoding and decoding of an delete table package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeDeleteTable() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final DeleteTableRequest deletePackage = new DeleteTableRequest(sequenceNumber, "test");
		
		byte[] encodedVersion = networkPackageToByte(deletePackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final DeleteTableRequest decodedPackage = DeleteTableRequest.decodeTuple(bb);
				
		Assert.assertEquals(deletePackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(deletePackage, decodedPackage);
	}
	
	
	/**
	 * Test decoding and encoding of the key query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testDecodeKeyQuery() throws IOException, PackageEncodeException {
		final String table = "1_mygroup_table1";
		final String key = "key1";
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryKeyRequest queryKeyRequest = new QueryKeyRequest(sequenceNumber, table, key);
		final byte[] encodedPackage = networkPackageToByte(queryKeyRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryKeyRequest decodedPackage = QueryKeyRequest.decodeTuple(bb);
		Assert.assertEquals(queryKeyRequest.getKey(), decodedPackage.getKey());
		Assert.assertEquals(queryKeyRequest.getTable(), decodedPackage.getTable());
		
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_KEY, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	}
	
	/**
	 * Test decode bounding box query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testDecodeBoundingBoxQuery() throws IOException, PackageEncodeException {
		final String table = "table1";
		final BoundingBox boundingBox = new BoundingBox(10d, 20d);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryBoundingBoxRequest queryRequest = new QueryBoundingBoxRequest(sequenceNumber, table, boundingBox, false, (short) 10);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryBoundingBoxRequest decodedPackage = QueryBoundingBoxRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getBoundingBox(), decodedPackage.getBoundingBox());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_BBOX, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	}
	
	/**
	 * Test decode time query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testDecodeTimeQuery() throws IOException, PackageEncodeException {
		final String table = "table1";
		final long timeStamp = 4711;
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryTimeRequest queryRequest = new QueryTimeRequest(sequenceNumber, table, timeStamp, true, (short) 50);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryTimeRequest decodedPackage = QueryTimeRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_TIME, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	}
	
	
	/**
	 * Test decode time query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testDecodeBoundingBoxAndTime() throws IOException, PackageEncodeException {
		final String table = "table1";
		final long timeStamp = 4711;
		final BoundingBox boundingBox = new BoundingBox(10d, 20d);

		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryBoundingBoxTimeRequest queryRequest = new QueryBoundingBoxTimeRequest(sequenceNumber, table, boundingBox, timeStamp, true, (short) 50);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryBoundingBoxTimeRequest decodedPackage = QueryBoundingBoxTimeRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getBoundingBox(), decodedPackage.getBoundingBox());
		Assert.assertEquals(queryRequest.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_BBOX_AND_TIME, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	}
	
	/**
	 * The the encoding and decoding of a list tables package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeListTable() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final ListTablesRequest listPackage = new ListTablesRequest(sequenceNumber);
		
		byte[] encodedVersion = networkPackageToByte(listPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final ListTablesRequest decodedPackage = ListTablesRequest.decodeTuple(bb);
				
		Assert.assertEquals(listPackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of the request helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeHeloRequest1() throws IOException, PackageEncodeException {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final HelloRequest helloPackage = new HelloRequest(sequenceNumber, 2, peerCapabilities);
		
		byte[] encodedVersion = networkPackageToByte(helloPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final HelloRequest decodedPackage = HelloRequest.decodeRequest(bb);
				
		Assert.assertEquals(helloPackage, decodedPackage);
		Assert.assertFalse(decodedPackage.getPeerCapabilities().hasGZipCompression());
		Assert.assertFalse(helloPackage.getPeerCapabilities().hasGZipCompression());
	}
	
	/**
	 * The the encoding and decoding of the request helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeHeloRequest2() throws IOException, PackageEncodeException {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		peerCapabilities.setGZipCompression();
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final HelloRequest helloPackage = new HelloRequest(sequenceNumber, 2, peerCapabilities);
		
		byte[] encodedVersion = networkPackageToByte(helloPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final HelloRequest decodedPackage = HelloRequest.decodeRequest(bb);
		
		Assert.assertEquals(helloPackage, decodedPackage);
		Assert.assertTrue(decodedPackage.getPeerCapabilities().hasGZipCompression());
		Assert.assertTrue(helloPackage.getPeerCapabilities().hasGZipCompression());
	}
	
	/**
	 * The the encoding and decoding of the response helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeHeloResponse1() throws IOException, PackageEncodeException {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		final HelloResponse helloPackage = new HelloResponse(sequenceNumber, 2, peerCapabilities);
		
		byte[] encodedVersion = networkPackageToByte(helloPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final HelloResponse decodedPackage = HelloResponse.decodePackage(bb);
				
		Assert.assertEquals(helloPackage, decodedPackage);
		Assert.assertFalse(decodedPackage.getPeerCapabilities().hasGZipCompression());
		Assert.assertFalse(helloPackage.getPeerCapabilities().hasGZipCompression());
	}
	
	/**
	 * The the encoding and decoding of the response helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeHeloResponse2() throws IOException, PackageEncodeException {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		peerCapabilities.setGZipCompression();
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		final HelloResponse helloPackage = new HelloResponse(sequenceNumber, 2, peerCapabilities);
		
		byte[] encodedVersion = networkPackageToByte(helloPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final HelloResponse decodedPackage = HelloResponse.decodePackage(bb);
				
		Assert.assertEquals(helloPackage, decodedPackage);
		Assert.assertTrue(helloPackage.getPeerCapabilities().hasGZipCompression());
		Assert.assertTrue(decodedPackage.getPeerCapabilities().hasGZipCompression());
	}
	
	
	/**
	 * Decode an encoded package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testDecodePackage() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", BoundingBox.EMPTY_BOX, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, new SSTableName("test"), tuple);
		
		byte[] encodedPackage = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedPackage);
				
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		Assert.assertTrue(result);
	}	
	
	/**
	 * Get the sequence number from a package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testGetSequenceNumber() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", BoundingBox.EMPTY_BOX, "abc".getBytes(), 12);
		
		// Increment to avoid sequenceNumber = 0
		sequenceNumberGenerator.getNextSequenceNummber();
		sequenceNumberGenerator.getNextSequenceNummber();
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, new SSTableName("test"), tuple);

		byte[] encodedPackage = networkPackageToByte(insertPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		short packageSequencenUmber = NetworkPackageDecoder.getRequestIDFromRequestPackage(bb);
		
		Assert.assertEquals(sequenceNumber, packageSequencenUmber);		
	}
	
	/**
	 * Read the body length from a request package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testGetRequestBodyLength() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", BoundingBox.EMPTY_BOX, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, new SSTableName("test"), tuple);
		
		byte[] encodedPackage = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedPackage);
		
		// 18 Byte package header
		int calculatedBodyLength = encodedPackage.length - 18;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		long bodyLength = NetworkPackageDecoder.getBodyLengthFromRequestPackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
	
	/**
	 * Get the package type from the response
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test
	public void getPackageTypeFromResponse1() throws PackageEncodeException, IOException {
		final SuccessResponse response = new SuccessResponse((short) 2);
		final byte[] encodedPackage = networkPackageToByte(response);

		Assert.assertNotNull(encodedPackage);
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		Assert.assertEquals(NetworkConst.RESPONSE_TYPE_SUCCESS, NetworkPackageDecoder.getPackageTypeFromResponse(bb));
	}
	
	/**
	 * Get the package type from the response
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test
	public void getPackageTypeFromResponse2() throws PackageEncodeException, IOException {
		final SuccessResponse response = new SuccessResponse((short) 2, "abc");
		final byte[] encodedPackage = networkPackageToByte(response);
		
		Assert.assertNotNull(encodedPackage);
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		Assert.assertEquals(NetworkConst.RESPONSE_TYPE_SUCCESS, NetworkPackageDecoder.getPackageTypeFromResponse(bb));
	}
	
	/**
	 * Read the body length from a result package
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test
	public void testGetResultBodyLength1() throws PackageEncodeException, IOException {
		final SuccessResponse response = new SuccessResponse((short) 2);
		final byte[] encodedPackage = networkPackageToByte(response);
		
		Assert.assertNotNull(encodedPackage);
		
		// 2 Bytes message length
		final int calculatedBodyLength = 2;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final long bodyLength = NetworkPackageDecoder.getBodyLengthFromResponsePackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
	
	/**
	 * Read the body length from a result package
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test
	public void testGetResultBodyLength2() throws PackageEncodeException, IOException {
		final SuccessResponse response = new SuccessResponse((short) 2, "abc");
		final byte[] encodedPackage = networkPackageToByte(response);
		Assert.assertNotNull(encodedPackage);
		
		// 2 Byte (short) data length
		int calculatedBodyLength = 5;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		long bodyLength = NetworkPackageDecoder.getBodyLengthFromResponsePackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
	
	/**
	 * Try to encode and decode the list tables response
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test
	public void testListTablesResponse() throws PackageEncodeException, IOException {
		final List<SSTableName> tables = new ArrayList<SSTableName>();
		tables.add(new SSTableName("3_group1_table1"));
		tables.add(new SSTableName("3_group1_testtable"));
		tables.add(new SSTableName("3_group1_test4711"));
		tables.add(new SSTableName("3_group1_mytest57"));
		
		final ListTablesResponse response = new ListTablesResponse((short) 3, tables);
		final byte[] encodedPackage = networkPackageToByte(response);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final ListTablesResponse responseDecoded = ListTablesResponse.decodePackage(bb);
		final List<SSTableName> myTables = responseDecoded.getTables();
		Assert.assertEquals(tables, myTables);
		Assert.assertEquals(tables.size(), myTables.size());
	}
	
	/**
	 * Try to encode and decode the single tuple response 
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test
	public void testSingleTupleResponse() throws PackageEncodeException, IOException {
		final String tablename = "table1";
		final Tuple tuple = new Tuple("abc", BoundingBox.EMPTY_BOX, "databytes".getBytes());
		
		final TupleResponse singleTupleResponse = new TupleResponse((short) 4, tablename, tuple);
		final byte[] encodedPackage = networkPackageToByte(singleTupleResponse);
		
		Assert.assertNotNull(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final TupleResponse responseDecoded = TupleResponse.decodePackage(bb);
		Assert.assertEquals(singleTupleResponse.getTable(), responseDecoded.getTable());
		Assert.assertEquals(singleTupleResponse.getTuple(), responseDecoded.getTuple());		
	}

	/**
	 * Test the decoding and the encoding of an compressed request package
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testCompression1Request() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 12, Arrays.asList(new DistributedInstance[] { new DistributedInstance("node1:3445")}));
		final Tuple tuple = new Tuple("key", BoundingBox.EMPTY_BOX, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, routingHeader, new SSTableName("test"), tuple);
		Assert.assertEquals(routingHeader, insertPackage.getRoutingHeader());
		
		final CompressionEnvelopeRequest compressionPackage = new CompressionEnvelopeRequest(sequenceNumber, insertPackage, NetworkConst.COMPRESSION_TYPE_GZIP);
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		compressionPackage.writeToOutputStream(bos);
		bos.close();
		final byte[] encodedVersion = bos.toByteArray();
		
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		Assert.assertNotNull(bb);

		final byte[] uncompressedBytes = CompressionEnvelopeRequest.decodePackage(bb);
		final ByteBuffer uncompressedByteBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedBytes);
		
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(uncompressedByteBuffer);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(routingHeader, decodedPackage.getRoutingHeader());
		Assert.assertFalse(insertPackage.getRoutingHeader().equals(new RoutingHeader(false)));
		Assert.assertEquals(insertPackage, decodedPackage);
	}
	
	/**
	 * Test the decoding and the encoding of an compressed request package
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testCompression2Request() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = new RoutingHeader(true, (short) 12, Arrays.asList(new DistributedInstance[] { new DistributedInstance("node1:3445")}));
		final Tuple tuple = new Tuple("abcdefghijklmopqrstuvxyz", BoundingBox.EMPTY_BOX, "abcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyz".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, routingHeader, new SSTableName("test"), tuple);
		Assert.assertEquals(routingHeader, insertPackage.getRoutingHeader());
		
		final CompressionEnvelopeRequest compressionPackage = new CompressionEnvelopeRequest(sequenceNumber, insertPackage, NetworkConst.COMPRESSION_TYPE_GZIP);
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		compressionPackage.writeToOutputStream(bos);
		bos.close();
		final byte[] encodedVersion = bos.toByteArray();
		
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		Assert.assertNotNull(bb);

		final byte[] uncompressedBytes = CompressionEnvelopeRequest.decodePackage(bb);
		final ByteBuffer uncompressedByteBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedBytes);
		
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(uncompressedByteBuffer);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(routingHeader, decodedPackage.getRoutingHeader());
		Assert.assertFalse(insertPackage.getRoutingHeader().equals(new RoutingHeader(false)));
		Assert.assertEquals(insertPackage, decodedPackage);
	}
	
	/**
	 * Test the compression response
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testCompressionReponse1() throws IOException, PackageEncodeException {
		final String tablename = "table1";
		final Tuple tuple = new Tuple("abc", BoundingBox.EMPTY_BOX, "databytes".getBytes());
		
		final TupleResponse singleTupleResponse = new TupleResponse((short) 4, tablename, tuple);
		final CompressionEnvelopeResponse compressionEnvelopeResponse = new CompressionEnvelopeResponse(singleTupleResponse, NetworkConst.COMPRESSION_TYPE_GZIP);
		final byte[] encodedPackage = networkPackageToByte(compressionEnvelopeResponse);
		Assert.assertNotNull(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final byte[] uncompressedPackage = CompressionEnvelopeResponse.decodePackage(bb);
		
		final ByteBuffer uncompressedPackageBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedPackage);

		final TupleResponse responseDecoded = TupleResponse.decodePackage(uncompressedPackageBuffer);
		Assert.assertEquals(singleTupleResponse.getTable(), responseDecoded.getTable());
		Assert.assertEquals(singleTupleResponse.getTuple(), responseDecoded.getTuple());
	}
	
	/**
	 * Test the compression response
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void testCompressionReponse2() throws IOException, PackageEncodeException {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		peerCapabilities.setGZipCompression();
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		final HelloResponse helloPackage = new HelloResponse(sequenceNumber, 2, peerCapabilities);
		
		final CompressionEnvelopeResponse compressionEnvelopeResponse = new CompressionEnvelopeResponse(helloPackage, NetworkConst.COMPRESSION_TYPE_GZIP);
		final byte[] encodedPackage = networkPackageToByte(compressionEnvelopeResponse);
		Assert.assertNotNull(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final byte[] uncompressedPackage = CompressionEnvelopeResponse.decodePackage(bb);
		
		final ByteBuffer uncompressedPackageBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedPackage);

		final HelloResponse decodedPackage = HelloResponse.decodePackage(uncompressedPackageBuffer);
				
		Assert.assertEquals(helloPackage, decodedPackage);
		Assert.assertTrue(helloPackage.getPeerCapabilities().hasGZipCompression());
		Assert.assertTrue(decodedPackage.getPeerCapabilities().hasGZipCompression());
	}
	
	/**
	 * The the encoding and decoding of a keep alive package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test
	public void encodeAndDecodeKeepAlive() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final KeepAliveRequest keepAlivePackage = new KeepAliveRequest(sequenceNumber);
		
		byte[] encodedVersion = networkPackageToByte(keepAlivePackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final KeepAliveRequest decodedPackage = KeepAliveRequest.decodeTuple(bb);
				
		Assert.assertEquals(keepAlivePackage, decodedPackage);
	}
}
	
