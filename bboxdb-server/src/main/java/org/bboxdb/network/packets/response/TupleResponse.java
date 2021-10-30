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
package org.bboxdb.network.packets.response;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkResponsePacket;
import org.bboxdb.network.packets.NetworkTupleEncoderDecoder;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleAndTable;

public class TupleResponse extends NetworkResponsePacket {
	
	/**
	 * The table
	 */
	protected final String table;
	
	/**
	 * The tuple
	 */
	protected final Tuple tuple;

	public TupleResponse(final short sequenceNumber, final String table, final Tuple tuple) {
		super(sequenceNumber);
		this.table = table;
		this.tuple = tuple;
	}
	
	@Override
	public byte getPackageType() {
			return NetworkConst.RESPONSE_TYPE_TUPLE;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {
		
		try {
			final byte[] encodedBytes = NetworkTupleEncoderDecoder.encode(tuple, table);
			final long headerLength = appendResponsePackageHeader(encodedBytes.length, outputStream);
			outputStream.write(encodedBytes);
			
			return headerLength + encodedBytes.length;
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
	 */
	public static TupleResponse decodePackage(final ByteBuffer encodedPackage) throws PacketEncodeException {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_TUPLE);

		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		return new TupleResponse(requestId, tupleAndTable.getTable(), tupleAndTable.getTuple());
	}

	public String getTable() {
		return table;
	}

	public Tuple getTuple() {
		return tuple;
	}

	@Override
	public String toString() {
		return "TupleResponse [table=" + table + ", tuple=" + tuple + "]";
	}
	
}
