/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.network.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.bboxdb.network.NetworkConnectionState;
import org.bboxdb.network.NetworkConst;
import org.bboxdb.network.NetworkPackageDecoder;
import org.bboxdb.network.capabilities.PeerCapabilities;
import org.bboxdb.network.packages.NetworkResponsePackage;
import org.bboxdb.network.packages.PackageEncodeException;
import org.bboxdb.network.packages.response.CompressionEnvelopeResponse;
import org.bboxdb.network.packages.response.TupleResponse;
import org.bboxdb.network.routing.RoutingHeader;
import org.bboxdb.network.routing.RoutingHeaderParser;
import org.bboxdb.storage.entity.SSTableName;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.util.ExceptionSafeThread;
import org.bboxdb.util.StreamHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConnectionHandler extends ExceptionSafeThread {

	/**
	 * The client socket
	 */
	protected final Socket clientSocket;
	
	/**
	 * The output stream of the socket
	 */
	protected BufferedOutputStream outputStream;
	
	/**
	 * The input stream of the socket
	 */
	protected InputStream inputStream;
	
	/**
	 * The connection state
	 */
	protected volatile NetworkConnectionState connectionState;

	/**
	 * The state of the server (read only or read write)
	 */
	private final NetworkConnectionServiceState networkConnectionServiceState;
	
	/**
	 * The capabilities of the connection
	 */
	private PeerCapabilities connectionCapabilities = new PeerCapabilities();
	
	/**
	 * The package handler
	 */
	protected final PackageHandler packageHandler = new PackageHandler(this);
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientConnectionHandler.class);

	public ClientConnectionHandler(final Socket clientSocket, final NetworkConnectionServiceState networkConnectionServiceState) {
		
		// The network connection state
		this.clientSocket = clientSocket;
		this.networkConnectionServiceState = networkConnectionServiceState;
		
		this.connectionState = NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING;
		
		try {
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());
			inputStream = new BufferedInputStream(clientSocket.getInputStream());
		} catch (IOException e) {
			inputStream = null;
			outputStream = null;
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.error("Exception while creating IO stream", e);
		}
	}

	/**
	 * Read the next package header from the socket
	 * @return The package header, wrapped in a ByteBuffer
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	protected ByteBuffer readNextPackageHeader(final InputStream inputStream) throws IOException, PackageEncodeException {
		final ByteBuffer bb = ByteBuffer.allocate(12);
		StreamHelper.readExactlyBytes(inputStream, bb.array(), 0, bb.limit());
		
		final RoutingHeader routingHeader = RoutingHeaderParser.decodeRoutingHeader(inputStream);
		final byte[] routingHeaderBytes = RoutingHeaderParser.encodeHeader(routingHeader);
		
		final ByteBuffer header = ByteBuffer.allocate(bb.limit() + routingHeaderBytes.length);
		header.put(bb.array());
		header.put(routingHeaderBytes);
		
		return header;
	}

	/**
	 * Write a response package to the client
	 * @param responsePackage
	 */
	public synchronized boolean writeResultPackage(final NetworkResponsePackage responsePackage) {
		
		try {
			responsePackage.writeToOutputStream(outputStream);
			outputStream.flush();
			
			return true;
		} catch (IOException | PackageEncodeException e) {
			logger.warn("Unable to write result package", e);
		}

		return false;
	}
	
	@Override
	public void runThread() {
		try {
			logger.debug("Handling new connection from: " + clientSocket.getInetAddress());

			while(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN ||
					connectionState == NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING) {
				handleNextPackage(inputStream);
			}
			
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.info("Closing connection to: {}", clientSocket.getInetAddress());
		} catch (IOException | PackageEncodeException e) {
			// Ignore exception on closing sockets
			if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				
				logger.error("Socket to {} closed unexpectly (state: {}), closing connection",
						clientSocket.getInetAddress(), connectionState);
				
				logger.debug("Socket exception", e);
			}
		} 
				
		closePackageHandlerNE();	
		closeSocketNE();
	}

	/**
	 * Close the package handler without throwing an exception
	 */
	protected void closePackageHandlerNE() {
		try {
			packageHandler.close();
		} catch (IOException e) {
			// Ignore close exception
		}
	}

	/**
	 * Close the socket without throwing an exception
	 */
	protected void closeSocketNE() {
		try {
			clientSocket.close();
		} catch (IOException e) {
			// Ignore close exception
		}
	}
	
	/**
	 * Read the full package. The total length of the package is read from the package header.
	 * @param packageHeader
	 * @return
	 * @throws IOException 
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader, 
			final InputStream inputStream) throws IOException {
		
		final int bodyLength = (int) NetworkPackageDecoder.getBodyLengthFromRequestPackage(packageHeader);
		final int headerLength = packageHeader.limit();
		
		final ByteBuffer encodedPackage = ByteBuffer.allocate(headerLength + bodyLength);
		
		try {
			//System.out.println("Trying to read: " + bodyLength + " avail " + in.available());			
			encodedPackage.put(packageHeader.array());
			StreamHelper.readExactlyBytes(inputStream, encodedPackage.array(), encodedPackage.position(), bodyLength);
		} catch (IOException e) {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
			throw e;
		}
		
		return encodedPackage;
	}

	/**
	 * Handle the next request package
	 * @throws IOException
	 * @throws PackageEncodeException 
	 */
	protected void handleNextPackage(final InputStream inputStream) throws IOException, PackageEncodeException {
		final ByteBuffer packageHeader = readNextPackageHeader(inputStream);
		
		final short packageSequence = NetworkPackageDecoder.getRequestIDFromRequestPackage(packageHeader);
		final short packageType = NetworkPackageDecoder.getPackageTypeFromRequest(packageHeader);
		
		if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_HANDSHAKING) {
			if(packageType != NetworkConst.REQUEST_TYPE_HELLO) {
				logger.error("Connection is in handshake state but got package: " + packageType);
				connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				return;
			}
		}
		
		final ByteBuffer encodedPackage = readFullPackage(packageHeader, inputStream);

		final boolean readFurtherPackages = packageHandler.handleBufferedPackage(encodedPackage, packageSequence, packageType);

		if(readFurtherPackages == false) {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
		}	
	}

	/**
	 * Send a new result tuple to the client (compressed or uncompressed)
	 * @param packageSequence
	 * @param requestTable
	 * @param tuple
	 */
	protected void writeResultTuple(final short packageSequence, final SSTableName requestTable, final Tuple tuple) {
		final TupleResponse responsePackage = new TupleResponse(packageSequence, requestTable.getFullname(), tuple);
		
		if(getConnectionCapabilities().hasGZipCompression()) {
			final CompressionEnvelopeResponse compressionEnvelopeResponse 
				= new CompressionEnvelopeResponse(NetworkConst.COMPRESSION_TYPE_GZIP, 
						Arrays.asList(responsePackage));
			
			writeResultPackage(compressionEnvelopeResponse);
		} else {
			writeResultPackage(responsePackage);
		}
	}

	/**
	 * Get the connection Capabilities
	 * @return
	 */
	public PeerCapabilities getConnectionCapabilities() {
		return connectionCapabilities;
	}

	/**
	 * Set the connection Capabilities
	 * @return
	 */
	public void setConnectionCapabilities(PeerCapabilities connectionCapabilities) {
		this.connectionCapabilities = connectionCapabilities;
	}

	/**
	 * Get the network connection state
	 * @return
	 */
	public NetworkConnectionServiceState getNetworkConnectionServiceState() {
		return networkConnectionServiceState;
	}
}