package de.fernunihagen.dna.jkn.scalephant;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.Test;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.SequenceNumberGenerator;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTableRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.InsertTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.ListTablesRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.SuccessResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.SuccessWithBodyResponse;
import de.fernunihagen.dna.jkn.scalephant.storage.BoundingBox;

public class TestNetworkClasses {
	
	/**
	 * The sequence number generator for our packages
	 */
	protected SequenceNumberGenerator sequenceNumberGenerator = new SequenceNumberGenerator();
	
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
	 */
	@Test
	public void testRequestPackageHeader() {
		final short currentSequenceNumber = sequenceNumberGenerator.getSequeneNumberWithoutIncrement();
		
		final NetworkPackageEncoder networkPackageBuilder = new NetworkPackageEncoder();
		final ByteArrayOutputStream encodedPackageStream
			= networkPackageBuilder.getOutputStreamForRequestPackage(sequenceNumberGenerator, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		
		final byte[] encodedPackage = encodedPackageStream.toByteArray();
		
		Assert.assertTrue(encodedPackage.length == 4);
		
		final ByteBuffer bb = ByteBuffer.wrap(encodedPackage);
		bb.order(NetworkConst.NETWORK_BYTEORDER);
		
		// Check fields
		Assert.assertEquals(NetworkConst.PROTOCOL_VERSION, bb.get());
		Assert.assertEquals(NetworkConst.REQUEST_TYPE_INSERT_TUPLE, bb.get());
		Assert.assertEquals(currentSequenceNumber, bb.getShort());
	}
	
	/**
	 * The the encoding and decoding of an insert tuple package
	 */
	@Test
	public void encodeAndDecodeInsertTuple() {
				
		final InsertTupleRequest insertPackage = new InsertTupleRequest("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		
		byte[] encodedVersion = insertPackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedVersion);

		final InsertTupleRequest decodedPackage = InsertTupleRequest.decodeTuple(encodedVersion);
				
		Assert.assertEquals(insertPackage.getKey(), decodedPackage.getKey());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(insertPackage.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(insertPackage.getBbox(), decodedPackage.getBbox());
		Assert.assertTrue(Arrays.equals(insertPackage.getData(), decodedPackage.getData()));
		
		Assert.assertEquals(insertPackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of an delete tuple package
	 */
	@Test
	public void encodeAndDecodeDeleteTuple() {
				
		final DeleteTupleRequest deletePackage = new DeleteTupleRequest("test", "key");
		
		byte[] encodedVersion = deletePackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedVersion);

		final DeleteTupleRequest decodedPackage = DeleteTupleRequest.decodeTuple(encodedVersion);
				
		Assert.assertEquals(deletePackage.getKey(), decodedPackage.getKey());
		Assert.assertEquals(deletePackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(deletePackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of an delete table package
	 */
	@Test
	public void encodeAndDecodeDeleteTable() {
				
		final DeleteTableRequest deletePackage = new DeleteTableRequest("test");
		
		byte[] encodedVersion = deletePackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedVersion);

		final DeleteTableRequest decodedPackage = DeleteTableRequest.decodeTuple(encodedVersion);
				
		Assert.assertEquals(deletePackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(deletePackage, decodedPackage);
	}
	
	/**
	 * The the encoding and decoding of a list tables package
	 */
	@Test
	public void encodeAndDecodeListTable() {
				
		final ListTablesRequest listPackage = new ListTablesRequest();
		
		byte[] encodedVersion = listPackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedVersion);

		final ListTablesRequest decodedPackage = ListTablesRequest.decodeTuple(encodedVersion);
				
		Assert.assertEquals(listPackage, decodedPackage);
	}
	
	/**
	 * Decode an encoded package
	 */
	@Test
	public void testDecodePackage() {
		final InsertTupleRequest insertPackage = new InsertTupleRequest("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		
		byte[] encodedPackage = insertPackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedPackage);
				
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		Assert.assertTrue(result);
	}
	
	/**
	 * Get the sequence number from a package
	 */
	@Test
	public void testGetSequenceNumber() {
		final InsertTupleRequest insertPackage = new InsertTupleRequest("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		
		// Increment to avoid sequenceNumber = 0
		sequenceNumberGenerator.getNextSequenceNummber();
		sequenceNumberGenerator.getNextSequenceNummber();
		
		short curSequencenUmber = sequenceNumberGenerator.getSequeneNumberWithoutIncrement();
		
		byte[] encodedPackage = insertPackage.getByteArray(sequenceNumberGenerator);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		short packageSequencenUmber = NetworkPackageDecoder.getRequestIDFromRequestPackage(bb);
		
		Assert.assertEquals(curSequencenUmber, packageSequencenUmber);		
	}
	
	/**
	 * Read the body length from a request package
	 */
	@Test
	public void testGetRequestBodyLength() {
		final InsertTupleRequest insertPackage = new InsertTupleRequest("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		byte[] encodedPackage = insertPackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedPackage);
		
		// 8 Byte package header
		int calculatedBodyLength = encodedPackage.length - 8;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		int bodyLength = NetworkPackageDecoder.getBodyLengthFromRequestPackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
	
	/**
	 * Get the package type from the response
	 */
	@Test
	public void getPackageTypeFromResponse1() {
		final SuccessResponse response = new SuccessResponse((short) 2);
		byte[] encodedPackage = response.getByteArray();
		Assert.assertNotNull(encodedPackage);
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		Assert.assertEquals(NetworkConst.RESPONSE_SUCCESS, NetworkPackageDecoder.getPackageTypeFromResponse(bb));
	}
	
	/**
	 * Get the package type from the response
	 */
	@Test
	public void getPackageTypeFromResponse2() {
		final SuccessWithBodyResponse response = new SuccessWithBodyResponse((short) 2, "abc");
		byte[] encodedPackage = response.getByteArray();
		Assert.assertNotNull(encodedPackage);
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		Assert.assertEquals(NetworkConst.RESPONSE_SUCCESS_WITH_BODY, NetworkPackageDecoder.getPackageTypeFromResponse(bb));
	}
	
	/**
	 * Read the body length from a result package
	 */
	@Test
	public void testGetResultBodyLength1() {
		final SuccessResponse response = new SuccessResponse((short) 2);
		byte[] encodedPackage = response.getByteArray();
		Assert.assertNotNull(encodedPackage);
		
		// 8 Byte package header
		int calculatedBodyLength = 0;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		int bodyLength = NetworkPackageDecoder.getBodyLengthFromResponsePackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
	
	/**
	 * Read the body length from a result package
	 */
	@Test
	public void testGetResultBodyLength2() {
		final SuccessWithBodyResponse response = new SuccessWithBodyResponse((short) 2, "abc");
		byte[] encodedPackage = response.getByteArray();
		Assert.assertNotNull(encodedPackage);
		
		// 8 Byte package header
		int calculatedBodyLength = 3;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		int bodyLength = NetworkPackageDecoder.getBodyLengthFromResponsePackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
	
}
