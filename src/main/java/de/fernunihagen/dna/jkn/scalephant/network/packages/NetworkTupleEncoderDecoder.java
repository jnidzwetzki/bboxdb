package de.fernunihagen.dna.jkn.scalephant.network.packages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.TupleAndTable;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableHelper;

public class NetworkTupleEncoderDecoder {
	
	/**
	 * Convert a ByteBuffer into a TupleAndTable object
	 * @param encodedPackage
	 * @return
	 */
	public static TupleAndTable decode(final ByteBuffer encodedPackage) {
		final short tableLength = encodedPackage.getShort();
		final short keyLength = encodedPackage.getShort();
		final int bBoxLength = encodedPackage.getInt();
		final int dataLength = encodedPackage.getInt();
		final long timestamp = encodedPackage.getLong();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] keyBytes = new byte[keyLength];
		encodedPackage.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);
		
		final byte[] boxBytes = new byte[bBoxLength];
		encodedPackage.get(boxBytes, 0, boxBytes.length);

		final byte[] dataBytes = new byte[dataLength];
		encodedPackage.get(dataBytes, 0, dataBytes.length);
		
		final float[] floatArray = SSTableHelper.readIEEE754FloatArrayFromByte(boxBytes);
		final BoundingBox boundingBox = new BoundingBox(floatArray);
		
		final Tuple tuple = new Tuple(key, boundingBox, dataBytes, timestamp);
		
		return new TupleAndTable(tuple, table);
	}
	
	/**
	 * Write the tuple and the table onto a ByteArrayOutputStream
	 * @param bos
	 * @param tuple
	 * @param table
	 * @throws IOException
	 */
	public static void encode(final ByteArrayOutputStream bos, final Tuple tuple, final String table) throws IOException {
		final byte[] tableBytes = table.getBytes();
		final byte[] keyBytes = tuple.getKey().getBytes();
		final byte[] bboxBytes = tuple.getBoundingBoxBytes();
		
		final ByteBuffer bb = ByteBuffer.allocate(20);
		bb.order(NetworkConst.NETWORK_BYTEORDER);
		bb.putShort((short) tableBytes.length);
		bb.putShort((short) keyBytes.length);
		bb.putInt(bboxBytes.length);
		bb.putInt(tuple.getDataBytes().length);
		bb.putLong(tuple.getTimestamp());
		
		// Write body length
		final int bodyLength = bb.capacity() + tableBytes.length 
				+ keyBytes.length + bboxBytes.length + tuple.getDataBytes().length;
		
		final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
		bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
		bodyLengthBuffer.putInt(bodyLength);
		bos.write(bodyLengthBuffer.array());
		
		// Write body
		bos.write(bb.array());
		bos.write(tableBytes);
		bos.write(keyBytes);
		bos.write(bboxBytes);
		bos.write(tuple.getDataBytes());
	}

}
