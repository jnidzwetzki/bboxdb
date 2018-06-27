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
package org.bboxdb.network.packages;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.bboxdb.misc.Const;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHeaderParser;

public abstract class NetworkRequestPackage extends NetworkPackage {

	/**
	 * The routing handler
	 */
	private final RoutingHeader routingHeader;

	public NetworkRequestPackage(short sequenceNumber, final RoutingHeader routingHeader) {
		super(sequenceNumber);
		this.routingHeader = Objects.requireNonNull(routingHeader);
	}

	public NetworkRequestPackage(final short sequenceNumber) {
		this(sequenceNumber, new RoutingHeader(false));
	}

	/**
	 * Append the request package header to the output stream
	 * @param bodyLength
	 * @param sequenceNumberGenerator
	 * @param packageType
	 * @param bos
	 * @return
	 * @throws PackageEncodeException
	 */
	protected int appendRequestPackageHeader(final long bodyLength, final OutputStream bos)
			throws PackageEncodeException {

		final ByteBuffer byteBuffer = ByteBuffer.allocate(12);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		byteBuffer.putShort(sequenceNumber);
		byteBuffer.putShort(getPackageType());
		byteBuffer.putLong(bodyLength);

		try {
			bos.write(byteBuffer.array());

			// Write routing header
			final byte[] routingHeaderBytes = RoutingHeaderParser.encodeHeader(getRoutingHeader());
			bos.write(routingHeaderBytes);

			return byteBuffer.capacity() + routingHeaderBytes.length;
		} catch (IOException e) {
			throw new PackageEncodeException(e);
		}
	}

	/**
	 * Get the routing header
	 * @return
	 * @throws PackageEncodeException
	 */
	public RoutingHeader getRoutingHeader() throws PackageEncodeException {
		return routingHeader;
	}

	/**
	 * Need the package to be canceled before it is retried
	 * @return
	 */
	public boolean needsToBeCanceled() {
		return false;
	}

	/**
	 * Needs this package an immediate connection flush
	 * (like queries or locks) or can this package queued?
	 * @return
	 */
	public boolean needsImmediateFlush() {
		return false;
	}
}
