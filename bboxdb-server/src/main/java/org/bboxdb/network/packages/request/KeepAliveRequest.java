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
import java.util.HashMap;
import java.util.Map;

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.util.DataEncoderHelper;

public class KeepAliveRequest extends NetworkRequestPackage {
	
	/**
	 * The keep alive tablename
	 */
	private final String tablename;
	
	/**
	 * The keep alive hash values (key, hash value)
	 */
	private final Map<String, Long> hashValues;

	public KeepAliveRequest(final short sequenceNumber) {
		this(sequenceNumber, "", new HashMap<>());
	}
	
	public KeepAliveRequest(final short sequenceNumber, final String tablename, 
			final Map<String, Long> hashValues) {
		
		super(sequenceNumber);
		this.tablename = tablename;
		this.hashValues = hashValues;
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
			bb.putInt(hashValues.size());
			
			final ByteArrayOutputStream bos = new ByteArrayOutputStream();
			bos.write(bb.array());
			bos.write(tableBytes);
			
			for(final String key : hashValues.keySet()) {
				final byte[] keyArray = key.getBytes();
				final ByteBuffer keyLengthBytes = DataEncoderHelper.intToByteBuffer(keyArray.length);
				final ByteBuffer checksumBytes = DataEncoderHelper.longToByteBuffer(hashValues.get(key));
				bos.write(keyLengthBytes.array());
				bos.write(keyArray);
				bos.write(checksumBytes.array());
			}
			
			bos.flush();
			bos.close();
			
			final byte[] bodyBytes = bos.toByteArray();
			final int bodyLength = bodyBytes.length;
			
			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			final long headerLength = appendRequestPackageHeader(bodyLength, routingHeader, outputStream);

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
		
		final Map<String, Long> hashValue = new HashMap<>();
		final short tableLength = encodedPackage.getShort();
		encodedPackage.get(); // Unused
		encodedPackage.get(); // Unused
		final int hashValues = encodedPackage.getInt();
		
		final byte[] tableNameBytes = new byte[tableLength];
		encodedPackage.get(tableNameBytes, 0, tableNameBytes.length);
		final String tableName = new String(tableNameBytes);
		
		for(int i = 0; i < hashValues; i++) {
			final int keyLength = encodedPackage.getInt();
			final byte[] keyBytes = new byte[keyLength];
			encodedPackage.get(keyBytes, 0, keyBytes.length);
			final String key = new String(keyBytes);
			final long value = encodedPackage.getLong();
			hashValue.put(key, value);
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new KeepAliveRequest(sequenceNumber, tableName, hashValue);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_KEEP_ALIVE;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hashValues == null) ? 0 : hashValues.hashCode());
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
		if (hashValues == null) {
			if (other.hashValues != null)
				return false;
		} else if (!hashValues.equals(other.hashValues))
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
	
	public Map<String, Long> getHashValues() {
		return hashValues;
	}
}
