/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
import java.util.EnumSet;

import org.bboxdb.commons.io.DataEncoderHelper;
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
	private final TupleStoreName table;
	
	/**
	 * The Tuple
	 */
	private final Tuple tuple;

	/**
	 * The insert options
	 */
	private EnumSet<InsertOption> insertOptions;

	/**
	 * Create package from parameter
	 * 
	 * @param table
	 * @param key
	 * @param timestamp
	 * @param bbox
	 * @param data
	 */
	public InsertTupleRequest(final short sequenceNumber, final RoutingHeader routingHeader, 
			final TupleStoreName table, final Tuple tuple, final EnumSet<InsertOption> insertOptions) {
		
		super(sequenceNumber, routingHeader);
		
		this.table = table;
		this.tuple = tuple;
		this.insertOptions = insertOptions;
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
		
		final int insertOptionsInt = encodedPackage.getInt();
		final EnumSet<InsertOption> insertOptions = EnumSet.noneOf(InsertOption.class);
		for(final InsertOption insertOption : InsertOption.values()) {
			if((insertOption.getStatusFlagValue() & insertOptionsInt) == insertOption.getStatusFlagValue()) {
				insertOptions.add(insertOption);
			}
		}
		
		final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);

		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);

		final TupleStoreName ssTableName = new TupleStoreName(tupleAndTable.getTable());
		
		final Tuple readTuple = tupleAndTable.getTuple();
		
		return new InsertTupleRequest(sequenceNumber, routingHeader, ssTableName, readTuple, insertOptions);
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			
			int optionsInt = 0;
		    for(final InsertOption option : insertOptions) {
		    	optionsInt |= option.getStatusFlagValue();
		    }
			
			final ByteBuffer optionsByte = DataEncoderHelper.intToByteBuffer(optionsInt);
			
			final byte[] tupleAsByte = NetworkTupleEncoderDecoder.encode(tuple, table.getFullname());
			
			// Body length
			final long bodyLength = tupleAsByte.length + optionsByte.limit();
			
			// Unrouted package
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write tuple
			outputStream.write(optionsByte.array());
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
	public String toString() {
		return "InsertTupleRequest [table=" + table + ", tuple=" + tuple 
				+ ", insertOptions=" + insertOptions + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((insertOptions == null) ? 0 : insertOptions.hashCode());
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
		if (insertOptions == null) {
			if (other.insertOptions != null)
				return false;
		} else if (!insertOptions.equals(other.insertOptions))
			return false;
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
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_INSERT_TUPLE;
	}
	
	public EnumSet<InsertOption> getInsertOptions() {
		return insertOptions;
	}

}

