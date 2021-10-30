/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.network.packets.request;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkQueryRequestPacket;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.TupleStoreName;

public class QueryInsertTimeRequest extends NetworkQueryRequestPacket {
	
	/**
	 * The name of the table
	 */
	protected final TupleStoreName table;

	/**
	 * The timestamp
	 */
	protected final long timestamp;
	
	/**
	 * Paging enables
	 */
	protected final boolean pagingEnabled;
	
	/**
	 * The max tuples per page
	 */
	protected final short tuplesPerPage;

	public QueryInsertTimeRequest(final short packageSequene, final RoutingHeader routingHeader, 
			final String table, final long timestamp, final boolean pagingEnabled, 
			final short tuplesPerPage) {
		
		super(packageSequene, routingHeader);
		
		this.table = new TupleStoreName(table);
		this.timestamp = timestamp;
		this.pagingEnabled = pagingEnabled;
		this.tuplesPerPage = tuplesPerPage;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			
			final ByteBuffer bb = ByteBuffer.allocate(14);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			
			bb.put(getQueryType());

			if(pagingEnabled) {
				bb.put((byte) 1);
			} else {
				bb.put((byte) 0);
			}
			
			bb.putShort(tuplesPerPage);
			bb.putLong(timestamp);
			bb.putShort((short) tableBytes.length);
			
			final long bodyLength = bb.capacity() + tableBytes.length;
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);
			
			return headerLength + bodyLength;
		} catch (IOException e) {
			throw new PacketEncodeException("Got exception while converting package into bytes", e);
		}	
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PacketEncodeException 
	 * @throws IOException 
	 */
	public static QueryInsertTimeRequest decodeTuple(final ByteBuffer encodedPackage) throws PacketEncodeException, IOException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_QUERY);
		
		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
	    final byte queryType = encodedPackage.get();
	    
	    if(queryType != NetworkConst.REQUEST_QUERY_INSERT_TIME) {
	    	throw new PacketEncodeException("Wrong query type: " + queryType);
	    }
		
	    boolean pagingEnabled = false;
	    if(encodedPackage.get() != 0) {
	    	pagingEnabled = true;
	    }
	    
	    final short tuplesPerPage = encodedPackage.getShort();
	    
	    final long timestamp = encodedPackage.getLong();
		final short tableLength = encodedPackage.getShort();
		
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);

		return new QueryInsertTimeRequest(sequenceNumber, routingHeader, table, timestamp, 
				pagingEnabled, tuplesPerPage);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_QUERY;
	}

	@Override
	public byte getQueryType() {
		return NetworkConst.REQUEST_QUERY_INSERT_TIME;
	}
	
	public TupleStoreName getTable() {
		return table;
	}

	public long getTimestamp() {
		return timestamp;
	}
	
	public short getTuplesPerPage() {
		return tuplesPerPage;
	}
	
	public boolean isPagingEnabled() {
		return pagingEnabled;
	}

	@Override
	public String toString() {
		return "QueryInsertTimeRequest [table=" + table + ", timestamp=" + timestamp + ", pagingEnabled="
				+ pagingEnabled + ", tuplesPerPage=" + tuplesPerPage + "]";
	}
}
