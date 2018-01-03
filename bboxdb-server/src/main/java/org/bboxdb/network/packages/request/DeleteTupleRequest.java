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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.TupleStoreName;


public class DeleteTupleRequest extends NetworkRequestPackage {

	/**
	 * The name of the table
	 */
	protected final TupleStoreName table;
	
	/**
	 * The key to delete
	 */
	protected final String key;
	
	/**
	 * The timestmap of the operation
	 */
	protected final long timestamp;
	
	/**
	 * A routing header for custom routing
	 */
	protected final RoutingHeader routingHeader;

	public DeleteTupleRequest(final short sequenceNumber, final RoutingHeader routingHeader, 
			final String table, final String key, final long timestamp) {
		
		super(sequenceNumber);
		
		this.table = new TupleStoreName(table);
		this.routingHeader = routingHeader;
		this.key = key;
		this.timestamp = timestamp;
	}

	/**
	 * Get the a encoded version of this class
	 * @return 
	 * @throws PackageEncodeException 
	 */
	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			final byte[] keyBytes = key.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(12);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) keyBytes.length);
			bb.putLong(timestamp);
			
			// Write body length
			final long bodyLength = bb.capacity() + tableBytes.length 
					+ keyBytes.length;
			
			final long headerLength = appendRequestPackageHeader(bodyLength, routingHeader, outputStream);
	
			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);
			outputStream.write(keyBytes);
			
			return headerLength + bodyLength;
		} catch (IOException e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}
	}
	
	/**
	 * Decode the encoded package into an object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public static DeleteTupleRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeException, IOException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_DELETE_TUPLE);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		final short tableLength = encodedPackage.getShort();
		final short keyLength = encodedPackage.getShort();
		final long timestamp = encodedPackage.getLong();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] keyBytes = new byte[keyLength];
		encodedPackage.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);

		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);
		
		return new DeleteTupleRequest(sequenceNumber, routingHeader, table, key, timestamp);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DELETE_TUPLE;
	}

	public TupleStoreName getTable() {
		return table;
	}

	public String getKey() {
		return key;
	}
	
	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DeleteTupleRequest other = (DeleteTupleRequest) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (timestamp != other.timestamp)
			return false;
		return true;
	}
	
	/**
	 * Get the routing header
	 * @return
	 */
	public RoutingHeader getRoutingHeader() {
		return routingHeader;
	}
}
