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
import de.fernunihagen.dna.jkn.scalephant.network.packages.InsertTuplePackage;
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
		
		final NetworkPackageEncoder networkPackageBuilder = new NetworkPackageEncoder(sequenceNumberGenerator);
		final ByteArrayOutputStream encodedPackageStream
			= networkPackageBuilder.getByteOutputStream(NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		
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
				
		final InsertTuplePackage insertPackage = new InsertTuplePackage("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		
		byte[] encodedVersion = insertPackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedVersion);

		final InsertTuplePackage decodedPackage = InsertTuplePackage.decodeTuple(encodedVersion);
				
		Assert.assertEquals(insertPackage.getKey(), decodedPackage.getKey());
		Assert.assertEquals(insertPackage.getTable(), decodedPackage.getTable());
		Assert.assertEquals(insertPackage.getTimestamp(), decodedPackage.getTimestamp());
		Assert.assertEquals(insertPackage.getBbox(), decodedPackage.getBbox());
		Assert.assertTrue(Arrays.equals(insertPackage.getData(), decodedPackage.getData()));
		
		Assert.assertEquals(insertPackage, decodedPackage);
	}
	
	/**
	 * Decode an encoded package
	 */
	@Test
	public void testDecodePackage() {
		final InsertTuplePackage insertPackage = new InsertTuplePackage("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		
		byte[] encodedPackage = insertPackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedPackage);
				
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		boolean result = NetworkPackageDecoder.validatePackageHeader(bb);
		Assert.assertTrue(result);
	}
	
	/**
	 * Get the sequence number from a package
	 */
	@Test
	public void testGetSequenceNumber() {
		final InsertTuplePackage insertPackage = new InsertTuplePackage("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		
		// Increment to avoid sequenceNumber = 0
		sequenceNumberGenerator.getNextSequenceNummber();
		sequenceNumberGenerator.getNextSequenceNummber();
		
		short curSequencenUmber = sequenceNumberGenerator.getSequeneNumberWithoutIncrement();
		
		byte[] encodedPackage = insertPackage.getByteArray(sequenceNumberGenerator);
		
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		short packageSequencenUmber = NetworkPackageDecoder.getRequestIDFromPackage(bb);
		
		Assert.assertEquals(curSequencenUmber, packageSequencenUmber);		
	}
	
	/**
	 * Read the body length from a package
	 */
	@Test
	public void testGetBodyLength() {
		final InsertTuplePackage insertPackage = new InsertTuplePackage("test", "key", 12, BoundingBox.EMPTY_BOX, "abc".getBytes());
		byte[] encodedPackage = insertPackage.getByteArray(sequenceNumberGenerator);
		Assert.assertNotNull(encodedPackage);
		
		// 8 Byte package header
		int calculatedBodyLength = encodedPackage.length - 8;
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		int bodyLength = NetworkPackageDecoder.getBodyLengthFromPackage(bb);
		
		Assert.assertEquals(calculatedBodyLength, bodyLength);
	}
}
