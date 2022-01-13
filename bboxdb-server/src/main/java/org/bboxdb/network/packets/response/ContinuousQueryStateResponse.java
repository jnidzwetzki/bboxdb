/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.entity.ContinuousQueryServerState;
import org.bboxdb.network.packets.NetworkResponsePacket;
import org.bboxdb.network.packets.PacketEncodeException;

public class ContinuousQueryStateResponse extends NetworkResponsePacket {
	
	/**
	 * The state
	 */
	private final ContinuousQueryServerState continuousQueryServerState;

	public ContinuousQueryStateResponse(final short sequenceNumber, final ContinuousQueryServerState continuousQueryServerState) {
		super(sequenceNumber);
		this.continuousQueryServerState = continuousQueryServerState;
	}
	
	@Override
	public byte getPackageType() {
			return NetworkConst.RESPONSE_CONTINUOUS_QUERY_STATE;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PacketEncodeException {
		
		try {
			final byte[] bodyBytes = createBody();			
			final long headerLength = appendResponsePackageHeader(bodyBytes.length, outputStream);
			outputStream.write(bodyBytes);
			
			return headerLength + bodyBytes.length;
		} catch (IOException e) {
			throw new PacketEncodeException("Got exception while converting package into bytes", e);
		}
		
	}
	
	/**
	 * Create the body as byte array
	 * @return
	 * @throws IOException
	 */
	protected byte[] createBody() throws IOException {
		final ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
		
		// The global states
		final Map<String, Set<String>> globalActiveRangeQueryElements = continuousQueryServerState.getGlobalActiveRangeQueryElements();
		final Map<String, Map<String, Set<String>>> globalActiveJoinElements = continuousQueryServerState.getGlobalActiveJoinElements();
		
		final List<String> allQueries = new ArrayList<>();
		allQueries.addAll(globalActiveJoinElements.keySet());
		allQueries.addAll(globalActiveRangeQueryElements.keySet());
		
		// Number of queries
		final int numberOfQueries = allQueries.size();
		final ByteBuffer totalTables = DataEncoderHelper.intToByteBuffer(numberOfQueries);			
		bodyStream.write(totalTables.array(), 0, totalTables.array().length);
		
		// Iterate over uuid
		for(final String queryUUID : allQueries) {
			
			final byte[] queryUUIDBytes = queryUUID.getBytes(Const.DEFAULT_CHARSET);	
			final ByteBuffer lengthOfQueryUUIDBytes = DataEncoderHelper.intToByteBuffer(queryUUIDBytes.length);			
			bodyStream.write(lengthOfQueryUUIDBytes.array(), 0, lengthOfQueryUUIDBytes.array().length);
			bodyStream.write(queryUUIDBytes, 0, queryUUIDBytes.length);
			
			// Range query keys
			final Set<String> activeRangeQueryElements = globalActiveRangeQueryElements.getOrDefault(queryUUID, new HashSet<>());

			final int numberOfRangeQueryKeys = activeRangeQueryElements.size();
			final ByteBuffer numberOfRangeQueryKeysBytes = DataEncoderHelper.intToByteBuffer(numberOfRangeQueryKeys);			
			bodyStream.write(numberOfRangeQueryKeysBytes.array(), 0, numberOfRangeQueryKeysBytes.array().length);
			
			for(final String activeRangeQueryElement : activeRangeQueryElements) {
				final byte[] keyBytes = activeRangeQueryElement.getBytes(Const.DEFAULT_CHARSET);
				final ByteBuffer keyLengthBytes = DataEncoderHelper.intToByteBuffer(keyBytes.length);			

				bodyStream.write(keyLengthBytes.array(), 0, keyLengthBytes.array().length);
				bodyStream.write(keyBytes, 0, keyBytes.length);
			}
			
			// Join keys
			final Map<String, Set<String>> activeJoinQueryElements = globalActiveJoinElements.getOrDefault(queryUUID, new HashMap<>());
			final int numberOfJoinQueryKeys = activeJoinQueryElements.size();
			final ByteBuffer numberOfJoinQueryKeysBytes = DataEncoderHelper.intToByteBuffer(numberOfJoinQueryKeys);			
			bodyStream.write(numberOfJoinQueryKeysBytes.array(), 0, numberOfJoinQueryKeysBytes.array().length);
						
			for(final Entry<String, Set<String>> activeJoinQueryElement : activeJoinQueryElements.entrySet()) {
				final byte[] keyBytes = activeJoinQueryElement.getKey().getBytes(Const.DEFAULT_CHARSET);
				final ByteBuffer keyLengthBytes = DataEncoderHelper.intToByteBuffer(keyBytes.length);			
				bodyStream.write(keyLengthBytes.array(), 0, keyLengthBytes.array().length);
				bodyStream.write(keyBytes, 0, keyBytes.length);

				// Join partner
				final int numberOfJoinPartner = activeJoinQueryElement.getValue().size();
				final ByteBuffer numberOfJoinPartnerBytes = DataEncoderHelper.intToByteBuffer(numberOfJoinPartner);			
				bodyStream.write(numberOfJoinPartnerBytes.array(), 0, numberOfJoinPartnerBytes.array().length);

				for(final String joinPartner : activeJoinQueryElement.getValue()) {
					final byte[] joinPartnerBytes = joinPartner.getBytes(Const.DEFAULT_CHARSET);
					final ByteBuffer joinPartnerLenghtBytes = DataEncoderHelper.intToByteBuffer(joinPartnerBytes.length);			
					bodyStream.write(joinPartnerLenghtBytes.array(), 0, joinPartnerLenghtBytes.array().length);
					bodyStream.write(joinPartnerBytes, 0, joinPartnerBytes.length);
				}
			}
		}
		
		bodyStream.close();
		
		return bodyStream.toByteArray();
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PacketEncodeException 
	 */
	public static ContinuousQueryStateResponse decodePackage(final ByteBuffer encodedPackage) throws PacketEncodeException {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_CONTINUOUS_QUERY_STATE);

		if(decodeResult == false) {
			throw new PacketEncodeException("Unable to decode package");
		}
		
		final ContinuousQueryServerState continuousQueryServerState = new ContinuousQueryServerState();
		
		// Number of queries
		final int numberOfQueries = encodedPackage.getInt();
		for(int i = 0; i < numberOfQueries; i++) {
			final int lengthOfUUID = encodedPackage.getInt();
			final byte[] uuidBytes = new byte[lengthOfUUID];			
			encodedPackage.get(uuidBytes, 0, uuidBytes.length);
			final String uuid = new String(uuidBytes);
			
			// Range query elements
			final Set<String> keys = new HashSet<>();
			final int numberOfRangeQueries = encodedPackage.getInt();
			for(int rangeQuery = 0; rangeQuery < numberOfRangeQueries; rangeQuery++) {
				final int lengthOfKey = encodedPackage.getInt();
				final byte[] keyBytes = new byte[lengthOfKey];			
				encodedPackage.get(keyBytes, 0, keyBytes.length);
				final String key = new String(keyBytes);
				keys.add(key);
			}
			
			if(numberOfRangeQueries > 0) {
				continuousQueryServerState.addRangeQueryState(uuid, keys);
			}
			
			// Join query elements
			final Map<String, Set<String>> activeJoinElements = new HashMap<>();
			final int numberOfJoinKeys = encodedPackage.getInt();
			
			for(int joinKey = 0; joinKey < numberOfJoinKeys; joinKey++) {
				final int lengthOfKey = encodedPackage.getInt();
				final byte[] keyBytes = new byte[lengthOfKey];			
				encodedPackage.get(keyBytes, 0, keyBytes.length);
				final String key = new String(keyBytes);
				
				final int numberOfJoinPartners = encodedPackage.getInt();
				final Set<String> joinPartner = new HashSet<>();
				for(int joinPartnerId = 0; joinPartnerId < numberOfJoinPartners; joinPartnerId++) {
					final int lengthOfPartner = encodedPackage.getInt();
					final byte[] partnerBytes = new byte[lengthOfPartner];			
					encodedPackage.get(partnerBytes, 0, partnerBytes.length);
					final String partner = new String(partnerBytes);
					joinPartner.add(partner);
				}
				
				activeJoinElements.put(key, joinPartner);	
			}
			
			if(numberOfJoinKeys > 0) {
				continuousQueryServerState.addJoinQueryState(uuid, activeJoinElements);
			}
		}
		
		
		if(encodedPackage.remaining() != 0) {
			throw new PacketEncodeException("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		return new ContinuousQueryStateResponse(requestId, continuousQueryServerState);
	}

	public ContinuousQueryServerState getContinuousQueryServerState() {
		return continuousQueryServerState;
	}

	@Override
	public String toString() {
		return "ContinuousQueryStateResponse [continuousQueryServerState=" + continuousQueryServerState + "]";
	}
}
