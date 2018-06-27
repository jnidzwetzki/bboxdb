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

import org.bboxdb.misc.Const;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.packages.NetworkRequestPackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.routing.RoutingHeader;

public class LockTupleRequest extends NetworkRequestPackage {

	/**
	 * The tablename
	 */
	private final String tablename;

	/**
	 * The key
	 */
	private final String key;

	/**
	 * The version
	 */
	private final long version;

	/**
	 * Delete on timeout
	 */
	private final boolean deleteOnTimeout;

	public LockTupleRequest(final short sequenceNumber, final RoutingHeader routingHeader,
			final String tablename, final String key, final long version, final boolean deleteOnTimeout) {

		super(sequenceNumber, routingHeader);
		this.tablename = tablename;
		this.key = key;
		this.version = version;
		this.deleteOnTimeout = deleteOnTimeout;
	}


	@Override
	public long writeToOutputStream(final OutputStream outputStream) throws PackageEncodeException {

		try {
			final byte[] tablenameBytes = tablename.getBytes();
			final byte[] keyBytes = key.getBytes();

			final ByteBuffer bb = ByteBuffer.allocate(16);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putShort((short) tablenameBytes.length);
			bb.putShort((short) keyBytes.length);

			if(deleteOnTimeout) {
				bb.put((byte) 1);
			} else {
				bb.put((byte) 0);
			}

			// Three unused bytes
			bb.put((byte) 0);
			bb.put((byte) 0);
			bb.put((byte) 0);

			bb.putLong(version);

			// Body length
			final long bodyLength = bb.capacity() + tablenameBytes.length
					+ keyBytes.length;

			final long headerLength = appendRequestPackageHeader(bodyLength, outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(tablenameBytes);
			outputStream.write(keyBytes);

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
	public static LockTupleRequest decodeTuple(final ByteBuffer encodedPackage)
			throws PackageEncodeException, IOException {

		final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromRequestPackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_LOCK_TUPLE);

		if(decodeResult == false) {
			throw new PackageEncodeException("Unable to decode package");
		}

		final short tablenameLength = encodedPackage.getShort();
		final short keyLength = encodedPackage.getShort();
		final boolean deleteOnTimeout = encodedPackage.get() == 1 ? true : false;

		// 3 unused bytes
		encodedPackage.get();
		encodedPackage.get();
		encodedPackage.get();

		final long version = encodedPackage.getLong();

		// Tablename
		final byte[] tableBytes = new byte[tablenameLength];
		encodedPackage.get(tableBytes, 0, tableBytes.length);
		final String tablename = new String(tableBytes);

		// Key
		final byte[] keyBytes = new byte[keyLength];
		encodedPackage.get(keyBytes, 0, keyBytes.length);
		final String key = new String(keyBytes);

		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeException("Some bytes are left after decoding: " + encodedPackage.remaining());
		}

		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);

		return new LockTupleRequest(sequenceNumber, routingHeader, tablename, key, version, deleteOnTimeout);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_LOCK_TUPLE;
	}

	@Override
	public boolean needsImmediateFlush() {
		return true;
	}

	public String getTablename() {
		return tablename;
	}

	public String getKey() {
		return key;
	}

	public long getVersion() {
		return version;
	}

	public boolean isDeleteOnTimeout() {
		return deleteOnTimeout;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((tablename == null) ? 0 : tablename.hashCode());
		result = prime * result + (int) (version ^ (version >>> 32));
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LockTupleRequest other = (LockTupleRequest) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (tablename == null) {
			if (other.tablename != null)
				return false;
		} else if (!tablename.equals(other.tablename))
			return false;
		if (version != other.version)
			return false;
		return true;
	}
}
