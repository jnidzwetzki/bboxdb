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
package org.bboxdb.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.storage.entity.Tuple;

public class KeepAliveRequest extends NetworkRequestPackage {
	
	/**
	 * The keep alive tablename
	 */
	private final String tablename;
	
	/**
	 * The tuple versions
	 */
	private final List<Tuple> tuples;

	public KeepAliveRequest(final short sequenceNumber) {
		this(sequenceNumber, "", new ArrayList<>());
	}
	
	public KeepAliveRequest(final short sequenceNumber, final String tablename, 
			final List<Tuple> tuples) {
		
		super(sequenceNumber);
		this.tablename = tablename;
		this.tuples = tuples;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] tableBytes = tablename.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(8);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putShort((short) tableBytes.length);
			bb.put(NetworkConst.UNUSED_BYTE);
			bb.put(NetworkConst.UNUSED_BYTE);
			bb.putInt(tuples.size());
			
			final ByteArrayOutputStream bos = new ByteArrayOutputStream();
			bos.write(bb.array());
			bos.write(tableBytes);
			
			for(final Tuple tuple : tuples) {
				final byte[] keyByteArray = tuple.getKey().getBytes();
				final byte[] boundingBoxBytes = tuple.getBoundingBoxBytes();
				final ByteBuffer keyLengthBytes = DataEncoderHelper.intToByteBuffer(keyByteArray.length);
				final ByteBuffer boundingBoxLength = DataEncoderHelper.intToByteBuffer(boundingBoxBytes.length);
				final ByteBuffer versionBytes = DataEncoderHelper.longToByteBuffer(tuple.getVersionTimestamp());
				
				bos.write(keyLengthBytes.array());
				bos.write(keyByteArray);
				bos.write(boundingBoxLength.array());
				bos.write(boundingBoxBytes);
				bos.write(versionBytes.array());
			}
			
			bos.flush();
			bos.close();
			
			final byte[] bodyBytes = bos.toByteArray();
			final int bodyLength = bodyBytes.length;
			
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			outputStream.write(bodyBytes);
			
			return headerLength + bodyLength;
		} catch (Exception e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}	
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeException 
	 */
	public static KeepAliveRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeException {
		
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_KEEP_ALIVE);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		final List<Tuple> tuples = new ArrayList<>();
		final short tableLength = encodedPackage.getShort();
		encodedPackage.get(); // Unused
		encodedPackage.get(); // Unused
		final int elements = encodedPackage.getInt();
		
		final byte[] tableNameBytes = new byte[tableLength];
		encodedPackage.get(tableNameBytes, 0, tableNameBytes.length);
		final String tableName = new String(tableNameBytes);
		
		for(int i = 0; i < elements; i++) {
			final int keyLength = encodedPackage.getInt();
			final byte[] keyBytes = new byte[keyLength];
			encodedPackage.get(keyBytes, 0, keyBytes.length);
			final String key = new String(keyBytes);
			
			final int boundingBoxLength = encodedPackage.getInt();
			final byte[] boundingBoxBytes = new byte[boundingBoxLength];
			encodedPackage.get(boundingBoxBytes, 0, boundingBoxBytes.length);
			final BoundingBox boundingBox = BoundingBox.fromByteArray(boundingBoxBytes);
			
			final long version = encodedPackage.getLong();
			final Tuple tuple = new Tuple(key, boundingBox, "".getBytes(), version);
			tuples.add(tuple);
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new KeepAliveRequest(sequenceNumber, tableName, tuples);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_KEEP_ALIVE;
	}
	
	@Override
	public boolean canBeRetriedOnFailure() {
		return false;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tuples == null) ? 0 : tuples.hashCode());
		result = prime * result + ((tablename == null) ? 0 : tablename.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final KeepAliveRequest other = (KeepAliveRequest) obj;
		if (tuples == null) {
			if (other.tuples != null)
				return false;
		} else if (!tuples.equals(other.tuples))
			return false;
		if (tablename == null) {
			if (other.tablename != null)
				return false;
		} else if (!tablename.equals(other.tablename))
			return false;
		return true;
	}
	
	public String getTablename() {
		return tablename;
	}
	
	public List<Tuple> getTuples() {
		return tuples;
	}
}
