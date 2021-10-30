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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packets.NetworkResponsePacket;
import org.bboxdb.network.packets.NetworkTupleEncoderDecoder;
import org.bboxdb.network.packets.PacketEncodeException;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleAndTable;

public class MultiTupleResponse extends NetworkResponsePacket {
	
	/**
	 * The joined tuple
	 */
	private final MultiTuple joinedTuple;

	public MultiTupleResponse(final short sequenceNumber, final MultiTuple joinedTuple) {
		super(sequenceNumber);
		this.joinedTuple = joinedTuple;
	}
	
	@Override
	public byte getPackageType() {
			return NetworkConst.RESPONSE_TYPE_JOINED_TUPLE;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {

		try {
			final ByteArrayOutputStream bStream = new ByteArrayOutputStream();
			final ByteBuffer encodedTupleAmount = DataEncoderHelper.intToByteBuffer(joinedTuple.getNumberOfTuples());
			bStream.write(encodedTupleAmount.array());
			
			for(int i = 0; i < joinedTuple.getNumberOfTuples(); i++) {
				final byte[] encodedBytes = NetworkTupleEncoderDecoder.encode(joinedTuple.getTuple(i), 
						joinedTuple.getTupleStoreName(i));
				
				bStream.write(encodedBytes);
			}
			
			bStream.close();
			
			final byte[] allTupleBytes = bStream.toByteArray();
			final long headerLength = appendResponsePackageHeader(allTupleBytes.length, outputStream);
			outputStream.write(allTupleBytes);
			return headerLength + allTupleBytes.length;
			
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
	public static MultiTupleResponse decodePackage(final ByteBuffer encodedPackage) throws PacketEncodeException {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_JOINED_TUPLE);

		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		final int numberOfTuples = encodedPackage.getInt();
		
		final List<String> tupleStoreNames = new ArrayList<>();
		final List<Tuple> tuples = new ArrayList<>(numberOfTuples);
		
		for(int i = 0; i < numberOfTuples; i++) {
			final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);
			tupleStoreNames.add(tupleAndTable.getTable());
			tuples.add(tupleAndTable.getTuple());
		}
		
		final MultiTuple joinedTuple = new MultiTuple(tuples, tupleStoreNames);
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		return new MultiTupleResponse(requestId, joinedTuple);
	}

	public MultiTuple getJoinedTuple() {
		return joinedTuple;
	}
}
