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
package org.bboxdb.network.packages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleAndTable;
import org.bboxdb.storage.util.TupleHelper;

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
		
		final BoundingBox boundingBox = BoundingBox.fromByteArray(boxBytes);
		
		Tuple tuple = null;
		if(TupleHelper.isDeletedTuple(boxBytes, dataBytes)) {
			tuple = new DeletedTuple(key, timestamp);
		} else {
			tuple = new Tuple(key, boundingBox, dataBytes, timestamp);
		}
		
		return new TupleAndTable(tuple, table);
	}
	
	/**
	 * Write the tuple and the table onto a ByteArrayOutputStream
	 * @param bos
	 * @param routingHeader 
	 * @param tuple
	 * @param table
	 * @return 
	 * @throws IOException
	 */
	public static byte[] encode(final Tuple tuple, final String table) throws IOException {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		final byte[] tableBytes = table.getBytes();
		final byte[] keyBytes = tuple.getKey().getBytes();
		final byte[] bboxBytes = tuple.getBoundingBoxBytes();
		
		final ByteBuffer bb = ByteBuffer.allocate(20);
		bb.order(Const.APPLICATION_BYTE_ORDER);
		bb.putShort((short) tableBytes.length);
		bb.putShort((short) keyBytes.length);
		bb.putInt(bboxBytes.length);
		bb.putInt(tuple.getDataBytes().length);
		bb.putLong(tuple.getVersionTimestamp());

		// Write body
		bos.write(bb.array());
		bos.write(tableBytes);
		bos.write(keyBytes);
		bos.write(bboxBytes);
		bos.write(tuple.getDataBytes());
		
		bos.close();
		
		return bos.toByteArray();
	}
}