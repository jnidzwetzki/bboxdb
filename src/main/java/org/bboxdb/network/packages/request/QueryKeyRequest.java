/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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

import org.bboxdb.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.NetworkPackageEncoder;
import org.bboxdb.network.packages.NetworkQueryRequestPackage;
import org.bboxdb.network.packages.PackageEncodeError;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.SSTableName;

public class QueryKeyRequest implements NetworkQueryRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final SSTableName table;

	/**
	 * The name of the key
	 */
	protected final String key;

	public QueryKeyRequest(final String table, final String key) {
		this.table = new SSTableName(table);
		this.key = key;
	}

	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) throws PackageEncodeError {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			final byte[] keyBytes = key.getBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(5);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			
			bb.put(getQueryType());
			bb.putShort((short) tableBytes.length);
			bb.putShort((short) keyBytes.length);
			
			// Body length
			final long bodyLength = bb.capacity() + tableBytes.length + keyBytes.length;
			
			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, routingHeader, 
					getPackageType(), outputStream);
			
			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);
			outputStream.write(keyBytes);
		} catch (IOException e) {
			throw new PackageEncodeError("Got exception while converting package into bytes", e);
		}	
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeError 
	 */
	public static QueryKeyRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_QUERY);
		
		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
		}
		
	    final byte queryType = encodedPackage.get();
	    
	    if(queryType != NetworkConst.REQUEST_QUERY_KEY) {
	    	throw new PackageEncodeError("Wrong query type: " + queryType);
	    }
		
		final short tableLength = encodedPackage.getShort();
		final short keyLength = encodedPackage.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] keyBytes = new byte[keyLength];
		encodedPackage.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeError("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new QueryKeyRequest(table, key);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_QUERY;
	}

	@Override
	public byte getQueryType() {
		return NetworkConst.REQUEST_QUERY_KEY;
	}
	
	public SSTableName getTable() {
		return table;
	}

	public String getKey() {
		return key;
	}

}
