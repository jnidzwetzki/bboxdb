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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkQueryRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.storage.entity.TupleStoreName;

public class QueryJoinRequest extends NetworkQueryRequestPackage {

	/**
	 * The tables to join
	 */
	protected final List<TupleStoreName> tables;

	/**
	 * The the query bounding box
	 */
	protected final Hyperrectangle box;

	/**
	 * Paging enables
	 */
	protected final boolean pagingEnabled;
	
	/**
	 * The max tuples per page
	 */
	protected final short tuplesPerPage;
	
	/**
	 * The custom filter name
	 */
	private String customFilterName;

	/**
	 * The custom filter value
	 */
	private String customFilterValue;

	public QueryJoinRequest(final short sequenceNumber, final RoutingHeader routingHeader,  
			final List<TupleStoreName> tables, final Hyperrectangle box, final String customFilterName, 
			final String customFilterValue, final boolean pagingEnabled, final short tuplesPerPage) {
		
		super(sequenceNumber, routingHeader);
		
		this.tables = tables;
		this.box = box;
		this.pagingEnabled = pagingEnabled;
		this.tuplesPerPage = tuplesPerPage;
		this.customFilterName = customFilterName;
		this.customFilterValue = customFilterValue;
	}

	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {			
			final byte[] bboxBytes = box.toByteArray();
			
			final ByteBuffer bb = ByteBuffer.allocate(20);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			
			bb.put(getQueryType());
			
			if(pagingEnabled) {
				bb.put((byte) 1);
			} else {
				bb.put((byte) 0);
			}
			
			bb.putShort(tuplesPerPage);
			
			bb.putInt(tables.size());
			bb.putInt(bboxBytes.length);
			
			final byte[] customFilterBytes = customFilterName.getBytes();
			final byte[] customValueBytes = customFilterValue.getBytes();
			
			bb.putInt((int) customFilterBytes.length);
			bb.putInt((int) customValueBytes.length);
			
			final ByteArrayOutputStream bStream = new ByteArrayOutputStream();
			
			for(int i = 0; i < tables.size(); i++) {
				final byte[] tablename = tables.get(i).getFullnameBytes();
				bStream.write(DataEncoderHelper.shortToByteBuffer((short) tablename.length).array());
				bStream.write(tablename);
			}
			
			final byte[] tablesArray = bStream.toByteArray();

			final long bodyLength = bb.capacity() + tablesArray.length + bboxBytes.length 
					+ customFilterBytes.length + customValueBytes.length;
			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(bboxBytes);
			outputStream.write(customFilterBytes);
			outputStream.write(customValueBytes);
			outputStream.write(tablesArray);
			
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
	public static QueryJoinRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeException, IOException {
		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);
		
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_QUERY);
		
		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}
		
	    final byte queryType = encodedPackage.get();
	    
	    if(queryType != NetworkConst.REQUEST_QUERY_JOIN) {
	    		throw new PackageEncodeException("Wrong query type: " + queryType + " required type is: " + NetworkConst.REQUEST_QUERY_JOIN);
	    }
	    
	    boolean pagingEnabled = false;
	    if(encodedPackage.get() != 0) {
	    		pagingEnabled = true;
	    }
	    
	    final short tuplesPerPage = encodedPackage.getShort();	    
		final int numberOfTables = encodedPackage.getInt();
	    final int bboxLength = encodedPackage.getInt();
	    final int customFilterLength = encodedPackage.getInt();
	    final int customValueLength = encodedPackage.getInt();
	    
		final byte[] bboxBytes = new byte[bboxLength];
		encodedPackage.get(bboxBytes, 0, bboxBytes.length);
		final Hyperrectangle boundingBox = Hyperrectangle.fromByteArray(bboxBytes);
		
		final byte[] customFilterNameBytes = new byte[customFilterLength];
		encodedPackage.get(customFilterNameBytes, 0, customFilterNameBytes.length);
		final String customFilterName = new String(customFilterNameBytes);
		
		final byte[] customFilterValueBytes = new byte[customValueLength];
		encodedPackage.get(customFilterValueBytes, 0, customFilterValueBytes.length);
		final String customFilterValue = new String(customFilterValueBytes);
				
		final List<TupleStoreName> tableNames = new ArrayList<>();
		
		for(int i = 0; i < numberOfTables; i++) {
			final short tableNameLength = encodedPackage.getShort();
			final byte[] tableBytes = new byte[tableNameLength];
			encodedPackage.get(tableBytes, 0, tableBytes.length);
			final String tablename = new java.lang.String(tableBytes);
			tableNames.add(new TupleStoreName(tablename));
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);

		return new QueryJoinRequest(sequenceNumber, routingHeader, tableNames, boundingBox, 
				customFilterName, customFilterValue, pagingEnabled, tuplesPerPage);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_QUERY;
	}

	@Override
	public byte getQueryType() {
		return NetworkConst.REQUEST_QUERY_JOIN;
	}
	

	public Hyperrectangle getBoundingBox() {
		return box;
	}
	
	public List<TupleStoreName> getTables() {
		return tables;
	}

	public short getTuplesPerPage() {
		return tuplesPerPage;
	}

	public boolean isPagingEnabled() {
		return pagingEnabled;
	}

	public String getCustomFilterName() {
		return customFilterName;
	}
	
	public String getCustomFilterValue() {
		return customFilterValue;
	}

	@Override
	public String toString() {
		return "QueryJoinRequest [tables=" + tables + ", box=" + box + ", pagingEnabled=" + pagingEnabled
				+ ", tuplesPerPage=" + tuplesPerPage + ", customFilterName=" + customFilterName + ", customFilterValue="
				+ customFilterValue + "]";
	}
}
