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
package org.bboxdb.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkResponsePackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.storage.entity.TupleStoreName;

public class ListTablesResponse extends NetworkResponsePackage {
	
	/**
	 * The tables
	 */
	protected final List<TupleStoreName> tables;

	public ListTablesResponse(final short sequenceNumber, final List<TupleStoreName> allTables) {
		super(sequenceNumber);
		this.tables = allTables;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_LIST_TABLES;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {
		
		try {
			final byte[] bodyBytes = createBody();			
			final long headerLength = appendResponsePackageHeader(bodyBytes.length, outputStream);
			outputStream.write(bodyBytes);
			
			return headerLength + bodyBytes.length;
		} catch (IOException e) {
			throw new PackageEncodeException("Got exception while converting package into bytes", e);
		}	
	}

	/**
	 * Create the body as byte array
	 * @return
	 * @throws IOException
	 */
	protected byte[] createBody() throws IOException {
		final ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
		
		// Write total amount of tables
		final ByteBuffer totalTables = DataEncoderHelper.intToByteBuffer(tables.size());			
		bodyStream.write(totalTables.array(), 0, totalTables.array().length);
		
		for(final TupleStoreName table : tables) {
			final byte[] tableBytes = table.getFullnameBytes();
			
			// Write table length
			final ByteBuffer tableLength = DataEncoderHelper.shortToByteBuffer((short) tableBytes.length);			
			bodyStream.write(tableLength.array(), 0, tableLength.array().length);
			
			// Write table name
			bodyStream.write(tableBytes, 0, tableBytes.length);
		}
		bodyStream.close();
		
		return bodyStream.toByteArray();
	}

	/**
	 * Returns the relations of this request
	 * @return
	 */
	public List<TupleStoreName> getTables() {
		return Collections.unmodifiableList(tables);
	}

	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeException 
	 */
	public static ListTablesResponse decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeException {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_LIST_TABLES);

		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
		// Read the total amount of tables
		final int totalTables = encodedPackage.getInt();
		final List<TupleStoreName> tables = new ArrayList<TupleStoreName>(totalTables);

		// Read and decode tables
		for(short readTables = 0; readTables < totalTables; readTables++) {
			// Read table name length
			final short tableNameLength = encodedPackage.getShort();
			final byte[] tablenameBytes = new byte[tableNameLength];
			
			// Read table name and decode
			encodedPackage.get(tablenameBytes, 0, tablenameBytes.length);
			final String tablename = new String(tablenameBytes);
			final TupleStoreName sstableName = new TupleStoreName(tablename);
			tables.add(sstableName);
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new ListTablesResponse(requestId, tables);
	}
}
