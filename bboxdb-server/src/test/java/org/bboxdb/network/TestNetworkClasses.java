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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.distribution.membership.BBoxDBInstance;
import org.bboxdb.misc.Const;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.client.SequenceNumberGenerator;
import org.bboxdb.network.packages.NetworkPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.request.CancelRequest;
import org.bboxdb.network.packages.request.CompressionEnvelopeRequest;
import org.bboxdb.network.packages.request.CreateDistributionGroupRequest;
import org.bboxdb.network.packages.request.CreateTableRequest;
import org.bboxdb.network.packages.request.DeleteDistributionGroupRequest;
import org.bboxdb.network.packages.request.DeleteTableRequest;
import org.bboxdb.network.packages.request.DisconnectRequest;
import org.bboxdb.network.packages.request.HelloRequest;
import org.bboxdb.network.packages.request.InsertTupleRequest;
import org.bboxdb.network.packages.request.KeepAliveRequest;
import org.bboxdb.network.packages.request.LockTupleRequest;
import org.bboxdb.network.packages.request.NextPageRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleContinuousRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleRequest;
import org.bboxdb.network.packages.request.QueryHyperrectangleTimeRequest;
import org.bboxdb.network.packages.request.QueryInsertTimeRequest;
import org.bboxdb.network.packages.request.QueryJoinRequest;
import org.bboxdb.network.packages.request.QueryKeyRequest;
import org.bboxdb.network.packages.request.QueryVersionTimeRequest;
import org.bboxdb.network.packages.response.CompressionEnvelopeResponse;
import org.bboxdb.network.packages.response.HelloResponse;
import org.bboxdb.network.packages.response.JoinedTupleResponse;
import org.bboxdb.network.packages.response.ListTablesResponse;
import org.bboxdb.network.packages.response.SuccessResponse;
import org.bboxdb.network.packages.response.TupleLockedResponse;
import org.bboxdb.network.packages.response.TupleResponse;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHop;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.DistributionGroupConfiguration;
import org.bboxdb.storage.entity.DistributionGroupConfigurationBuilder;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreConfigurationBuilder;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.util.TupleHelper;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.io.ByteStreams;

public class TestNetworkClasses {
	
	/**
	 * The sequence number generator for our packages
	 */
	protected final SequenceNumberGenerator sequenceNumberGenerator = new SequenceNumberGenerator();
	
	/**
	 * A routing hop
	 */
	protected final RoutingHop ROUTING_HOP = new RoutingHop(new BBoxDBInstance("127.0.0.1:8080"), Arrays.asList(1l, 6l));
	
	/**
	 * A routing header - routed
	 */
	protected final RoutingHeader ROUTING_HEADER_ROUTED = new RoutingHeader((short) 0, Arrays.asList(ROUTING_HOP));

	/**
	 * A routing header - unrouted
	 */
	protected final RoutingHeader ROUTING_HEADER_UNROUTED = new RoutingHeader(false);
	
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
	 * Test the encoding of the request package header
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testRequestPackageHeader() throws IOException, PackageEncodeException {
		final short currentSequenceNumber = sequenceNumberGenerator.getSequeneNumberWithoutIncrement();
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		
		final KeepAliveRequest listTablesRequest = new KeepAliveRequest(sequenceNumber);
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		listTablesRequest.writeToOutputStream(bos);
		bos.flush();
		bos.close();

		final byte[] encodedPackage = bos.toByteArray();
		
		Assert.assertEquals(26, encodedPackage.length);
		
		final ByteBuffer bb = ByteBuffer.wrap(encodedPackage);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		
		// Check fields
		Assert.assertEquals(currentSequenceNumber, bb.getShort());
		Assert.assertEquals(NetworkConst.REQUEST_TYPE_KEEP_ALIVE, bb.getShort());
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeDeletedTuple1() throws IOException, PackageEncodeException {
		final Tuple tuple = new DeletedTuple("key", 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_UNROUTED, new TupleStoreName("test"), tuple);
		
		byte[] encodedVersion = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(bb);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(insertPackage.getRoutingHeader(), new RoutingHeader(false));
		Assert.assertEquals(insertPackage, decodedPackage);
		Assert.assertEquals(insertPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(insertPackage.toString(), decodedPackage.toString());

		Assert.assertTrue(TupleHelper.isDeletedTuple(decodedPackage.getTuple()));
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeInsertTuple1() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_UNROUTED, new TupleStoreName("test"), tuple);
		
		byte[] encodedVersion = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(bb);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(insertPackage.getRoutingHeader(), new RoutingHeader(false));
		Assert.assertEquals(insertPackage, decodedPackage);
		Assert.assertEquals(insertPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertTrue(insertPackage.toString().length() > 10);
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeInsertTuple2() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", new Hyperrectangle(1.3244343224, 232.232333343, 34324.343, 343243.0), "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_UNROUTED, new TupleStoreName("test"), tuple);
		
		byte[] encodedVersion = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(bb);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(insertPackage.getRoutingHeader(), new RoutingHeader(false));
		Assert.assertEquals(insertPackage, decodedPackage);
		Assert.assertEquals(insertPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertTrue(insertPackage.toString().length() > 10);
	}
	
	/**
	 * Test the decoding and the encoding of a joined tuple
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeJoinedTuple() throws IOException, PackageEncodeException {
		final Tuple tuple1 = new Tuple("key1", new Hyperrectangle(1.3244343224, 232.232333343, 34324.343, 343243.0), "abc".getBytes(), 12);
		final Tuple tuple2 = new Tuple("key2", new Hyperrectangle(1.32443453224, 545334.03, 34324.343, 343243.0), "abc".getBytes(), 12);
		final Tuple tuple3 = new Tuple("key3", new Hyperrectangle(1.35433224, 5453.43, 34324.343, 343243.0), "abc".getBytes(), 12);

		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final List<Tuple> tupleList = Arrays.asList(tuple1, tuple2, tuple3);
		final List<String> tableNames = Arrays.asList("abc", "def", "geh");
		
		final JoinedTuple joinedTuple = new JoinedTuple(tupleList, tableNames);
		
		final JoinedTupleResponse joinedResponse = new JoinedTupleResponse(sequenceNumber, joinedTuple);

		byte[] encodedVersion = networkPackageToByte(joinedResponse);
		Assert.assertNotNull(encodedVersion);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final JoinedTupleResponse decodedPackage = JoinedTupleResponse.decodePackage(bb);
		
		final JoinedTuple decodedJoinedTuple = decodedPackage.getJoinedTuple();
		Assert.assertEquals(3, decodedJoinedTuple.getNumberOfTuples());
				
		for(int i = 0; i < 3; i++) {
			Assert.assertEquals(tupleList.get(i), decodedJoinedTuple.getTuple(i));
			Assert.assertEquals(tableNames.get(i), decodedJoinedTuple.getTupleStoreName(i));
		}
		
		Assert.assertTrue(joinedResponse.toString().length() > 10);
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeInsertTupleWithCustomHeader() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = ROUTING_HEADER_ROUTED;
		final Tuple tuple = new Tuple("key", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_ROUTED, new TupleStoreName("test"), tuple);
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
		
		Assert.assertEquals(insertPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertTrue(insertPackage.toString().length() > 10);
	}
	
	/**
	 * The the encoding and decoding of an create distribution group package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeCreateDistributionGroup() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final DistributionGroupConfiguration distributionGroupConfiguration = DistributionGroupConfigurationBuilder.create(4)
				.withPlacementStrategy("abc", "def")
				.withSpacePartitioner("efg", "ijh")
				.withMaximumRegionSize(33333)
				.withMinimumRegionSize(1111)
				.withReplicationFactor((short) 11)
				.build();
				
		final CreateDistributionGroupRequest groupPackage = new CreateDistributionGroupRequest(sequenceNumber, 
				"test", distributionGroupConfiguration);
		
		byte[] encodedVersion = networkPackageToByte(groupPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final CreateDistributionGroupRequest decodedPackage = CreateDistributionGroupRequest.decodeTuple(bb);
				
		Assert.assertEquals(groupPackage.getDistributionGroup(), decodedPackage.getDistributionGroup());
		Assert.assertEquals(groupPackage.getDistributionGroupConfiguration(), distributionGroupConfiguration);
	
		Assert.assertEquals(groupPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(groupPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of an create distribution group package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeDeleteDistributionGroup() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final DeleteDistributionGroupRequest groupPackage = new DeleteDistributionGroupRequest(sequenceNumber, "test");
		
		byte[] encodedVersion = networkPackageToByte(groupPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final DeleteDistributionGroupRequest decodedPackage = DeleteDistributionGroupRequest.decodeTuple(bb);
				
		Assert.assertEquals(groupPackage.getDistributionGroup(), decodedPackage.getDistributionGroup());
		Assert.assertEquals(groupPackage, decodedPackage);
		
		Assert.assertEquals(groupPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(groupPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The encoding and decoding of an next page package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeNextPage() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final short querySequence = 12;
		final NextPageRequest nextPageRequest = new NextPageRequest(sequenceNumber, querySequence);
		
		byte[] encodedVersion = networkPackageToByte(nextPageRequest);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final NextPageRequest decodedPackage = NextPageRequest.decodeTuple(bb);
				
		Assert.assertEquals(decodedPackage.getQuerySequence(), querySequence);
		
		Assert.assertEquals(nextPageRequest.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(nextPageRequest.toString(), decodedPackage.toString());
	}
	
	
	/**
	 * The  encoding and decoding of an cancel query package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeCancelQuery() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final short querySequence = 12;
		final CancelRequest cancelQueryRequest = new CancelRequest(sequenceNumber, querySequence);
		
		byte[] encodedVersion = networkPackageToByte(cancelQueryRequest);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final CancelRequest decodedPackage = CancelRequest.decodeTuple(bb);
				
		Assert.assertEquals(decodedPackage.getQuerySequence(), querySequence);
		Assert.assertEquals(cancelQueryRequest.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(cancelQueryRequest.toString(), decodedPackage.toString());
	}
	
	/**
	 * The  encoding and decoding of an cancel query package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeLockTuple() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final LockTupleRequest request = new LockTupleRequest(sequenceNumber, ROUTING_HEADER_ROUTED, 
				"abc", "key1", 12, true);
		
		byte[] encodedVersion = networkPackageToByte(request);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final LockTupleRequest decodedPackage = LockTupleRequest.decodeTuple(bb);
		
		Assert.assertEquals(request.getKey(), decodedPackage.getKey());
		Assert.assertEquals(request.getTablename(), decodedPackage.getTablename());
		Assert.assertEquals(request.getVersion(), decodedPackage.getVersion());
		Assert.assertEquals(request.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(request.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of an delete table package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeDeleteTable() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final DeleteTableRequest deletePackage = new DeleteTableRequest(sequenceNumber, "test");
		
		byte[] encodedVersion = networkPackageToByte(deletePackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final DeleteTableRequest decodedPackage = DeleteTableRequest.decodeTuple(bb);
				
		Assert.assertEquals(deletePackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(deletePackage, decodedPackage);
		Assert.assertEquals(decodedPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(deletePackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of an create table package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeCreateTable() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final TupleStoreConfiguration ssTableConfiguration = TupleStoreConfigurationBuilder
				.create()
				.withTTL(10, TimeUnit.MILLISECONDS)
				.withVersions(666)
				.withSpatialIndexReader("reader")
				.withSpatialIndexWriter("writer")
				.build();
		
		final CreateTableRequest createPackage = new CreateTableRequest(sequenceNumber, "test", ssTableConfiguration);
		
		final byte[] encodedVersion = networkPackageToByte(createPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final CreateTableRequest decodedPackage = CreateTableRequest.decodeTuple(bb);
				
		Assert.assertEquals(createPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(createPackage.getTupleStoreConfiguration(), ssTableConfiguration);
		Assert.assertEquals(createPackage, decodedPackage);
		
		Assert.assertEquals(createPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(createPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * Test decoding and encoding of the key query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodeKeyQuery() throws IOException, PackageEncodeException {
		final String table = "1_mygroup_table1";
		final String key = "key1";
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryKeyRequest queryKeyRequest = new QueryKeyRequest(sequenceNumber, ROUTING_HEADER_UNROUTED, table, key, false, (short) 10);
		final byte[] encodedPackage = networkPackageToByte(queryKeyRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryKeyRequest decodedPackage = QueryKeyRequest.decodeTuple(bb);
		Assert.assertEquals(queryKeyRequest.getKey(), decodedPackage.getKey());
		Assert.assertEquals(queryKeyRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryKeyRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryKeyRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_KEY, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	
		Assert.assertEquals(queryKeyRequest.toString(), decodedPackage.toString());
	}
	
	/**
	 * Test decode bounding box query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodeBoundingBoxQuery() throws IOException, PackageEncodeException {
		final String table = "table1";
		final Hyperrectangle boundingBox = new Hyperrectangle(10d, 20d);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryHyperrectangleRequest queryRequest = new QueryHyperrectangleRequest(sequenceNumber, ROUTING_HEADER_ROUTED, table, boundingBox, false, (short) 10);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryHyperrectangleRequest decodedPackage = QueryHyperrectangleRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getBoundingBox(), decodedPackage.getBoundingBox());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_BBOX, NetworkPackageDecoder.getQueryTypeFromRequest(bb));

		Assert.assertEquals(queryRequest.toString(), decodedPackage.toString());
	}
	
	/**
	 * Test decode bounding box query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodeCointinousBoundingBoxQuery() throws IOException, PackageEncodeException {
		final String table = "table1";
		final Hyperrectangle boundingBox = new Hyperrectangle(10d, 20d);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryHyperrectangleContinuousRequest queryRequest = new QueryHyperrectangleContinuousRequest(sequenceNumber, ROUTING_HEADER_ROUTED, table, boundingBox);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryHyperrectangleContinuousRequest decodedPackage = QueryHyperrectangleContinuousRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getBoundingBox(), decodedPackage.getBoundingBox());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_CONTINUOUS_BBOX, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
		Assert.assertTrue(queryRequest.toString().length() > 10);
	}
	
	/**
	 * Test decode version time query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodeVersionTimeQuery() throws IOException, PackageEncodeException {
		final String table = "table1";
		final long timeStamp = 4711;
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryVersionTimeRequest queryRequest = new QueryVersionTimeRequest(sequenceNumber, ROUTING_HEADER_ROUTED, table, timeStamp, true, (short) 50);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryVersionTimeRequest decodedPackage = QueryVersionTimeRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_VERSION_TIME, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	
		Assert.assertTrue(queryRequest.toString().length() > 10);
	}
	
	/**
	 * Test decode insert time query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodeInsertTimeQuery() throws IOException, PackageEncodeException {
		final String table = "table1";
		final long timeStamp = 4711;
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryInsertTimeRequest queryRequest = new QueryInsertTimeRequest(sequenceNumber, ROUTING_HEADER_ROUTED, table, timeStamp, true, (short) 50);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryInsertTimeRequest decodedPackage = QueryInsertTimeRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_INSERT_TIME, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	
		Assert.assertEquals(queryRequest.toString(), decodedPackage.toString());
	}
	
	/**
	 * Test decode time query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodeBoundingBoxAndTime() throws IOException, PackageEncodeException {
		final String table = "table1";
		final long timeStamp = 4711;
		final Hyperrectangle boundingBox = new Hyperrectangle(10d, 20d);

		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryHyperrectangleTimeRequest queryRequest = new QueryHyperrectangleTimeRequest(sequenceNumber, ROUTING_HEADER_ROUTED, table, boundingBox, timeStamp, true, (short) 50);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		Assert.assertEquals(NetworkConst.REQUEST_QUERY_BBOX_AND_TIME, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
		
		final QueryHyperrectangleTimeRequest decodedPackage = QueryHyperrectangleTimeRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getBoundingBox(), decodedPackage.getBoundingBox());
		Assert.assertEquals(queryRequest.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(queryRequest.getTable(), decodedPackage.getTable());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		
		Assert.assertEquals(queryRequest.toString(), decodedPackage.toString());
	}
	
	/**
	 * Test decode bounding box query
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodeJoinQuery() throws IOException, PackageEncodeException {
		final List<TupleStoreName> tables = Arrays.asList(new TupleStoreName("3dgroup_table1"),
				new TupleStoreName("3dgroup_table2"), new TupleStoreName("3dgroup_table3"));
		
		final Hyperrectangle boundingBox = new Hyperrectangle(10d, 20d);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final QueryJoinRequest queryRequest = new QueryJoinRequest(sequenceNumber, ROUTING_HEADER_ROUTED, tables, boundingBox, false, (short) 10);
		byte[] encodedPackage = networkPackageToByte(queryRequest);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_QUERY);
		Assert.assertTrue(result);

		final QueryJoinRequest decodedPackage = QueryJoinRequest.decodeTuple(bb);
		Assert.assertEquals(queryRequest.getBoundingBox(), decodedPackage.getBoundingBox());
		Assert.assertEquals(tables, decodedPackage.getTables());
		Assert.assertEquals(queryRequest.isPagingEnabled(), decodedPackage.isPagingEnabled());
		Assert.assertEquals(queryRequest.getTuplesPerPage(), decodedPackage.getTuplesPerPage());
		Assert.assertEquals(NetworkConst.REQUEST_QUERY_JOIN, NetworkPackageDecoder.getQueryTypeFromRequest(bb));
	
		Assert.assertEquals(queryRequest.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of disconnect package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeDisconnect() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final DisconnectRequest listPackage = new DisconnectRequest(sequenceNumber);
		
		byte[] encodedVersion = networkPackageToByte(listPackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final DisconnectRequest decodedPackage = DisconnectRequest.decodeTuple(bb);
				
		Assert.assertEquals(listPackage, decodedPackage);
		
		Assert.assertEquals(listPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of the request helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
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
		
		Assert.assertEquals(helloPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(helloPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of the request helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
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
		
		Assert.assertEquals(helloPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(helloPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of the response helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
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
		
		Assert.assertEquals(helloPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(helloPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of the response helo
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
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
		
		Assert.assertEquals(helloPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(helloPackage.toString(), decodedPackage.toString());
	}
	
	
	/**
	 * Decode an encoded package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testDecodePackage() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_UNROUTED, new TupleStoreName("test"), tuple);
		
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
	@Test(timeout=60000)
	public void testGetSequenceNumber() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 12);
		
		// Increment to avoid sequenceNumber = 0
		sequenceNumberGenerator.getNextSequenceNummber();
		sequenceNumberGenerator.getNextSequenceNummber();
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_UNROUTED, new TupleStoreName("test"), tuple);

		final byte[] encodedPackage = networkPackageToByte(insertPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final short packageSequencenNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(bb);
		
		Assert.assertEquals(sequenceNumber, packageSequencenNumber);		
	}
	
	/**
	 * Read the body length from a request package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testGetRequestBodyLength() throws IOException, PackageEncodeException {
		final Tuple tuple = new Tuple("key", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_UNROUTED, new TupleStoreName("test"), tuple);
		
		final byte[] encodedPackage = networkPackageToByte(insertPackage);
		Assert.assertNotNull(encodedPackage);
		
		// 18 Byte package header
		final int calculatedBodyLength = encodedPackage.length - 18;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final long bodyLength = NetworkPackageDecoder.getBodyLengthFromRequestPackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
	
	/**
	 * Get the package type from the response
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test(timeout=60000)
	public void getPackageTypeLockedTupleSuccess() throws PackageEncodeException, IOException {
		final TupleLockedResponse response = new TupleLockedResponse((short) 2);
		final byte[] encodedPackage = networkPackageToByte(response);

		Assert.assertNotNull(encodedPackage);
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		Assert.assertEquals(NetworkConst.RESPONSE_TYPE_TUPLE_LOCK_SUCCESS, NetworkPackageDecoder.getPackageTypeFromResponse(bb));
	
		TupleLockedResponse.decodePackage(bb);
	}
	
	/**
	 * Get the package type from the response
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
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
	@Test(timeout=60000)
	public void testListTablesResponse() throws PackageEncodeException, IOException {
		final List<TupleStoreName> tables = new ArrayList<TupleStoreName>();
		tables.add(new TupleStoreName("3_group1_table1"));
		tables.add(new TupleStoreName("3_group1_testtable"));
		tables.add(new TupleStoreName("3_group1_test4711"));
		tables.add(new TupleStoreName("3_group1_mytest57"));
		
		final ListTablesResponse response = new ListTablesResponse((short) 3, tables);
		final byte[] encodedPackage = networkPackageToByte(response);
		Assert.assertNotNull(encodedPackage);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final ListTablesResponse responseDecoded = ListTablesResponse.decodePackage(bb);
		final List<TupleStoreName> myTables = responseDecoded.getTables();
		Assert.assertEquals(tables, myTables);
		Assert.assertEquals(tables.size(), myTables.size());
	}
	
	/**
	 * Try to encode and decode the single tuple response 
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test(timeout=60000)
	public void testSingleTupleResponse() throws PackageEncodeException, IOException {
		final String tablename = "table1";
		final Tuple tuple = new Tuple("abc", Hyperrectangle.FULL_SPACE, "databytes".getBytes());
		
		final TupleResponse singleTupleResponse = new TupleResponse((short) 4, tablename, tuple);
		final byte[] encodedPackage = networkPackageToByte(singleTupleResponse);
		
		Assert.assertNotNull(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final TupleResponse responseDecoded = TupleResponse.decodePackage(bb);
		Assert.assertEquals(singleTupleResponse.getTable(), responseDecoded.getTable());
		Assert.assertEquals(singleTupleResponse.getTuple(), responseDecoded.getTuple());
		Assert.assertFalse(singleTupleResponse.getTuple() instanceof DeletedTuple);
		
		Assert.assertTrue(singleTupleResponse.toString().length() > 10);
	}
	
	/**
	 * Try to encode and decode the single tuple response - with deleted tuple
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	@Test(timeout=60000)
	public void testSingleTupleDeletedResponse() throws PackageEncodeException, IOException {
		final String tablename = "table1";
		final Tuple tuple = new DeletedTuple("abc", 12);
		
		final TupleResponse singleTupleResponse = new TupleResponse((short) 4, tablename, tuple);
		final byte[] encodedPackage = networkPackageToByte(singleTupleResponse);
		
		Assert.assertNotNull(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final TupleResponse responseDecoded = TupleResponse.decodePackage(bb);
		Assert.assertEquals(singleTupleResponse.getTable(), responseDecoded.getTable());
		Assert.assertEquals(singleTupleResponse.getTuple(), responseDecoded.getTuple());	
		Assert.assertTrue(singleTupleResponse.getTuple() instanceof DeletedTuple);
		
		Assert.assertEquals(singleTupleResponse.toString(), responseDecoded.toString());
	}

	/**
	 * Test the decoding and the encoding of an compressed request package
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testCompression1Request() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = ROUTING_HEADER_ROUTED;
		
		final Tuple tuple = new Tuple("key", Hyperrectangle.FULL_SPACE, "abc".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_ROUTED, new TupleStoreName("test"), tuple);
		Assert.assertEquals(routingHeader, insertPackage.getRoutingHeader());
		
		final CompressionEnvelopeRequest compressionPackage = new CompressionEnvelopeRequest(NetworkConst.COMPRESSION_TYPE_GZIP, Arrays.asList(insertPackage));
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		compressionPackage.writeToOutputStream(bos);
		bos.close();
		final byte[] encodedVersion = bos.toByteArray();
		
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		Assert.assertNotNull(bb);

		final InputStream uncompressedByteStream = CompressionEnvelopeRequest.decodePackage(bb);
		final byte[] uncompressedBytes = ByteStreams.toByteArray(uncompressedByteStream);
		final ByteBuffer uncompressedByteBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedBytes);
		
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(uncompressedByteBuffer);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(routingHeader, decodedPackage.getRoutingHeader());
		Assert.assertFalse(insertPackage.getRoutingHeader().equals(new RoutingHeader(false)));
		Assert.assertEquals(insertPackage, decodedPackage);
		
		Assert.assertEquals(insertPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertTrue(insertPackage.toString().length() > 10);
	}
	
	/**
	 * Test the decoding and the encoding of an compressed request package
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testCompression2Request() throws IOException, PackageEncodeException {
		final RoutingHeader routingHeader = ROUTING_HEADER_ROUTED;
		final Tuple tuple = new Tuple("abcdefghijklmopqrstuvxyz", Hyperrectangle.FULL_SPACE, "abcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyzabcdefghijklmopqrstuvxyz".getBytes(), 12);
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final InsertTupleRequest insertPackage = new InsertTupleRequest(sequenceNumber, ROUTING_HEADER_ROUTED, new TupleStoreName("test"), tuple);
		Assert.assertEquals(routingHeader, insertPackage.getRoutingHeader());
		
		final CompressionEnvelopeRequest compressionPackage = new CompressionEnvelopeRequest(
				NetworkConst.COMPRESSION_TYPE_GZIP, Arrays.asList(insertPackage));
		
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		compressionPackage.writeToOutputStream(bos);
		bos.close();
		final byte[] encodedVersion = bos.toByteArray();
		
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		Assert.assertNotNull(bb);

		final InputStream uncompressedByteStream = CompressionEnvelopeRequest.decodePackage(bb);
		final byte[] uncompressedBytes = ByteStreams.toByteArray(uncompressedByteStream);
		final ByteBuffer uncompressedByteBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedBytes);
		
		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(uncompressedByteBuffer);
				
		Assert.assertEquals(insertPackage.getTuple(), decodedPackage.getTuple());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(routingHeader, decodedPackage.getRoutingHeader());
		Assert.assertFalse(insertPackage.getRoutingHeader().equals(new RoutingHeader(false)));
		Assert.assertEquals(insertPackage, decodedPackage);
		
		Assert.assertEquals(insertPackage.hashCode(), decodedPackage.hashCode());
		Assert.assertTrue(insertPackage.toString().length() > 10);
	}
	
	/**
	 * Test the compression response
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testCompressionReponse1() throws IOException, PackageEncodeException {
		final String tablename = "table1";
		final Tuple tuple = new Tuple("abc", Hyperrectangle.FULL_SPACE, "databytes".getBytes());
		
		final TupleResponse singleTupleResponse = new TupleResponse((short) 4, tablename, tuple);
		final CompressionEnvelopeResponse compressionEnvelopeResponse = new CompressionEnvelopeResponse(NetworkConst.COMPRESSION_TYPE_GZIP, Arrays.asList(singleTupleResponse));
		final byte[] encodedPackage = networkPackageToByte(compressionEnvelopeResponse);
		Assert.assertNotNull(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final InputStream uncompressedByteStream = CompressionEnvelopeResponse.decodePackage(bb);
		final byte[] uncompressedBytes = ByteStreams.toByteArray(uncompressedByteStream);
		final ByteBuffer uncompressedByteBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedBytes);

		final TupleResponse responseDecoded = TupleResponse.decodePackage(uncompressedByteBuffer);
		Assert.assertEquals(singleTupleResponse.getTable(), responseDecoded.getTable());
		Assert.assertEquals(singleTupleResponse.getTuple(), responseDecoded.getTuple());
		
		Assert.assertTrue(singleTupleResponse.toString().length() > 10);
	}
	
	/**
	 * Test the compression response
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void testCompressionReponse2() throws IOException, PackageEncodeException {
		final PeerCapabilities peerCapabilities = new PeerCapabilities();
		peerCapabilities.setGZipCompression();
		
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		final HelloResponse helloPackage = new HelloResponse(sequenceNumber, 2, peerCapabilities);
		
		final CompressionEnvelopeResponse compressionEnvelopeResponse = new CompressionEnvelopeResponse(NetworkConst.COMPRESSION_TYPE_GZIP, Arrays.asList(helloPackage));

		final byte[] encodedPackage = networkPackageToByte(compressionEnvelopeResponse);
		Assert.assertNotNull(encodedPackage);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final InputStream uncompressedByteStream = CompressionEnvelopeResponse.decodePackage(bb);
		
		final byte[] uncompressedBytes = ByteStreams.toByteArray(uncompressedByteStream);
		final ByteBuffer uncompressedByteBuffer = NetworkPackageDecoder.encapsulateBytes(uncompressedBytes);

		final HelloResponse decodedPackage = HelloResponse.decodePackage(uncompressedByteBuffer);
				
		Assert.assertEquals(helloPackage, decodedPackage);
		Assert.assertTrue(helloPackage.getPeerCapabilities().hasGZipCompression());
		Assert.assertTrue(decodedPackage.getPeerCapabilities().hasGZipCompression());
		
		Assert.assertEquals(helloPackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of a keep alive package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeKeepAlive1() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final KeepAliveRequest keepAlivePackage = new KeepAliveRequest(sequenceNumber);
		
		byte[] encodedVersion = networkPackageToByte(keepAlivePackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final KeepAliveRequest decodedPackage = KeepAliveRequest.decodeTuple(bb);
				
		Assert.assertEquals(keepAlivePackage, decodedPackage);
		
		Assert.assertEquals(keepAlivePackage.hashCode(), decodedPackage.hashCode());
		Assert.assertEquals(keepAlivePackage.toString(), decodedPackage.toString());
	}
	
	/**
	 * The the encoding and decoding of a keep alive package
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	@Test(timeout=60000)
	public void encodeAndDecodeKeepAlive2() throws IOException, PackageEncodeException {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();

		final String tablename = "testgroup_abc";
		
		final List<Tuple> tuples = new ArrayList<>();
		tuples.add(new Tuple("abc", new Hyperrectangle(1d, 2d, 1d, 2d), "".getBytes()));
		tuples.add(new Tuple("def", new Hyperrectangle(2d, 2d, 4d, 20d), "".getBytes()));
		tuples.add(new Tuple("xyz", new Hyperrectangle(10d, 20d, 10d, 20d), "".getBytes()));

		final KeepAliveRequest keepAlivePackage = new KeepAliveRequest(sequenceNumber, tablename, tuples);
		
		byte[] encodedVersion = networkPackageToByte(keepAlivePackage);
		Assert.assertNotNull(encodedVersion);

		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedVersion);
		final KeepAliveRequest decodedPackage = KeepAliveRequest.decodeTuple(bb);
				
		Assert.assertEquals(keepAlivePackage.getTablename(), decodedPackage.getTablename());
		Assert.assertEquals(keepAlivePackage.getTuples().size(), decodedPackage.getTuples().size());
	}
}
	
