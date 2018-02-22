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
import java.util.function.Supplier;

import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkQueryRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.TupleStoreName;

public class QueryBoundingBoxContinuousRequest extends NetworkQueryRequestPackage {

	/**
	 * The name of the table
	 */
	protected final TupleStoreName table;

	/**
	 * The the query bounding box
	 */
	protected final BoundingBox box;
	
	public QueryBoundingBoxContinuousRequest(final short sequenceNumber, 
			final Supplier<RoutingHeader> routingHeaderSupplier,  
			final String table, final BoundingBox box) {
		
		super(sequenceNumber, routingHeaderSupplier);
		
		this.table = new TupleStoreName(table);
		this.box = box;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			final byte[] bboxBytes = box.toByteArray();
			
			final ByteBuffer bb = ByteBuffer.allocate(8);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			
			bb.put(getQueryType());
			bb.put(NetworkConst.UNUSED_BYTE);
			bb.putShort((short) tableBytes.length);
			bb.putInt((int) bboxBytes.length);
			
			final long bodyLength = bb.capacity() + tableBytes.length + bboxBytes.length;
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);
			outputStream.write(bboxBytes);
			
			return headerLength + bodyLength;
		} catch (IOException e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}	
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeException 
	 * @throws IOException 
	 */
	public static QueryBoundingBoxContinuousRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeException, IOException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_QUERY);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
	    final byte queryType = encodedPackage.get();
	    
	    if(queryType != NetworkConst.REQUEST_QUERY_CONTINUOUS_BBOX) {
	    	throw new PackageEncodeException("Wrong query type: " + queryType + " required type is: " + NetworkConst.REQUEST_QUERY_CONTINUOUS_BBOX);
	    }
	    
	    // 1 unused byte
	    encodedPackage.get();
		final short tableLength = encodedPackage.getShort();
	    final int bboxLength = encodedPackage.getInt();

		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		final byte[] bboxBytes = new byte[bboxLength];
		encodedPackage.get(bboxBytes, 0, bboxBytes.length);
		final BoundingBox boundingBox = BoundingBox.fromByteArray(bboxBytes);
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);
		final Supplier<RoutingHeader> routingHeaderSupplier = () -> (routingHeader);

		return new QueryBoundingBoxContinuousRequest(sequenceNumber, routingHeaderSupplier, table, boundingBox);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_QUERY;
	}

	@Override
	public byte getQueryType() {
		return NetworkConst.REQUEST_QUERY_CONTINUOUS_BBOX;
	}
	
	public TupleStoreName getTable() {
		return table;
	}

	public BoundingBox getBoundingBox() {
		return box;
	}

}
