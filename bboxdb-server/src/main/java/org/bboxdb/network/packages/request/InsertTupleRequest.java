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

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.NetworkTupleEncoderDecoder;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleAndTable;
import org.bboxdb.storage.entity.TupleStoreName;

public class InsertTupleRequest extends NetworkRequestPackage {

	/**
	 * The name of the table
	 */
	protected final TupleStoreName table;
	
	/**
	 * The Tuple
	 */
	protected final Tuple tuple;

	/**
	 * Create package from parameter
	 * 
	 * @param table
	 * @param key
	 * @param timestamp
	 * @param bbox
	 * @param data
	 */
	public InsertTupleRequest(final short sequenceNumber, final Supplier<RoutingHeader> routingHeaderSupplier, 
			final TupleStoreName table, final Tuple tuple) {
		
		super(sequenceNumber, routingHeaderSupplier);
		
		this.table = table;
		this.tuple = tuple;
	}

	/**
	 * Decode the encoded tuple into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws IOException 
	 * @throws PackageEncodeException 
	 */
	public static InsertTupleRequest decodeTuple(final ByteBuffer encodedPackage) throws IOException, PackageEncodeException {

		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);

		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);
		
		final Supplier<RoutingHeader> routingHeaderSupplier = () -> {
			return routingHeader;
		};
		
		final TupleStoreName ssTableName = new TupleStoreName(tupleAndTable.getTable());
		return new InsertTupleRequest(sequenceNumber, routingHeaderSupplier, ssTableName, tupleAndTable.getTuple());
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] tupleAsByte = NetworkTupleEncoderDecoder.encode(tuple, table.getFullname());
			
			// Body length
			final long bodyLength = tupleAsByte.length;
			
			// Unrouted package
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write tuple
			outputStream.write(tupleAsByte);
			
			return headerLength + bodyLength;
		} catch (IOException e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}		
	}
	
	/**
	 * Get the referenced table
	 * @return
	 */
	public TupleStoreName getTable() {
		return table;
	}

	/**
	 * Get the referenced tuple
	 * @return
	 */
	public Tuple getTuple() {
		return tuple;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + ((tuple == null) ? 0 : tuple.hashCode());
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
		InsertTupleRequest other = (InsertTupleRequest) obj;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (tuple == null) {
			if (other.tuple != null)
				return false;
		} else if (!tuple.equals(other.tuple))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "InsertTupleRequest [table=" + table + ", tuple=" + tuple + "]";
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_INSERT_TUPLE;
	}

}
