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

import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.TupleStoreConfiguration;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.util.UpdateAnomalyResolver;

public class CreateTableRequest extends NetworkRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final TupleStoreName table;
	
	/**
	 * The configuration of the SSTable
	 */
	protected final TupleStoreConfiguration ssTableConfiguration;

	public CreateTableRequest(final short sequenceNumber, final String table, 
			final TupleStoreConfiguration ssTableConfiguration) {
		super(sequenceNumber);
		
		this.ssTableConfiguration = ssTableConfiguration;
		this.table = new TupleStoreName(table);
	}
	
	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] tableBytes = table.getFullnameBytes();
			final ByteBuffer bb = ByteBuffer.allocate(20);
			bb.putShort((short) tableBytes.length);
			
			if(ssTableConfiguration.isAllowDuplicates()) {
				bb.put((byte) 0x01);
			} else {
				bb.put((byte) 0x00);
			}
			
			// Update anomaly resolver
			bb.put(ssTableConfiguration.getUpdateAnomalyResolver().getValue());
			
			// TTL
			bb.putLong(ssTableConfiguration.getTTL());
			
			// Versions
			bb.putInt(ssTableConfiguration.getVersions());

			// Spatial index reader
			final byte[] spatialIndexReaderBytes = ssTableConfiguration.getSpatialIndexReader().getBytes();
			bb.putShort((short) spatialIndexReaderBytes.length);
			
			// Spatial index writer
			final byte[] spatialIndexWriterBytes = ssTableConfiguration.getSpatialIndexWriter().getBytes();
			bb.putShort((short) spatialIndexWriterBytes.length);
			
			// Body length
			final long bodyLength = bb.capacity() + tableBytes.length 
					+ spatialIndexReaderBytes.length + spatialIndexWriterBytes.length;
			
			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			final long headerLength = appendRequestPackageHeader(bodyLength, routingHeader, outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(tableBytes);
			outputStream.write(spatialIndexReaderBytes);
			outputStream.write(spatialIndexWriterBytes);
			
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
	 */
	public static CreateTableRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_CREATE_TABLE);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		// Table length
		final short tableLength = encodedPackage.getShort();		
		// Allow duplicates
		boolean allowDuplicates = false;
		
		if(encodedPackage.get() != 0) {
			allowDuplicates = true;
		}
		
		// Update anomyly resolver
		final byte updateAnomalyResolver = encodedPackage.get();
		
		// TTL
		final long ttl = encodedPackage.getLong();
		
		// Versions
		final int versions = encodedPackage.getInt();
		
		// Spatial reader length
		final short spatialReaderLength = encodedPackage.getShort();
		
		// Spatial writer length
		final short spatialWriterLength = encodedPackage.getShort();
		
		// Table name
		final byte[] tableBytes = new byte[tableLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String table = new String(tableBytes);
		
		// Spatial index reader
		final byte[] spatialReaderBytes = new byte[spatialReaderLength];
		encodedPackage.get(spatialReaderBytes, 0, spatialReaderBytes.length);
		final String spatialIndexReader = new String(spatialReaderBytes);
		
		// Spatial index writer
		final byte[] spatialWriterBytes = new byte[spatialWriterLength];
		encodedPackage.get(spatialWriterBytes, 0, spatialWriterBytes.length);
		final String spatialIndexWriter = new String(spatialWriterBytes);
				
		final TupleStoreConfiguration tupleStoreConfiguration = new TupleStoreConfiguration();
		tupleStoreConfiguration.setAllowDuplicates(allowDuplicates);
		tupleStoreConfiguration.setTtl(ttl);
		tupleStoreConfiguration.setVersions(versions);
		tupleStoreConfiguration.setSpatialIndexReader(spatialIndexReader);
		tupleStoreConfiguration.setSpatialIndexWriter(spatialIndexWriter);
		
		final UpdateAnomalyResolver updateAnomalyResolverEnum 
			= UpdateAnomalyResolver.buildFromByte(updateAnomalyResolver);
		tupleStoreConfiguration.setUpdateAnomalyResolver(updateAnomalyResolverEnum);
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new CreateTableRequest(sequenceNumber, table, tupleStoreConfiguration);
	}
	
	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_CREATE_TABLE;
	}

	public TupleStoreName getTable() {
		return table;
	}

	public TupleStoreConfiguration getTupleStoreConfiguration() {
		return ssTableConfiguration;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ssTableConfiguration == null) ? 0 : ssTableConfiguration.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
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
		CreateTableRequest other = (CreateTableRequest) obj;
		if (ssTableConfiguration == null) {
			if (other.ssTableConfiguration != null)
				return false;
		} else if (!ssTableConfiguration.equals(other.ssTableConfiguration))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		return true;
	}

}
