package de.fernunihagen.dna.jkn.scalephant.network.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTableRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.SuccessResponse;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageInterface;

public class ClientConnectionHandler implements Runnable {
	
	/**
	 * The client socket
	 */
	protected final Socket clientSocket;
	
	/**
	 * The output stream of the socket
	 */
	protected BufferedOutputStream out;
	
	/**
	 * The input stream of the socket
	 */
	protected BufferedInputStream in;
	
	/**
	 * The connection state
	 */
	protected volatile NetworkConnectionState connectionState;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientConnectionHandler.class);
	

	public ClientConnectionHandler(final Socket clientSocket) {
		this.clientSocket = clientSocket;
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPEN;
		
		try {
			out = new BufferedOutputStream(clientSocket.getOutputStream());
			in = new BufferedInputStream(clientSocket.getInputStream());
		} catch (IOException e) {
			in = null;
			out = null;
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.error("Exception while creating IO stream", e);
		}
	}

	/**
	 * Read the next pakage header from the socket
	 * @return The package header, wrapped in a ByteBuffer
	 * @throws IOException
	 */
	protected ByteBuffer readNextPackageHeader() throws IOException {
		final ByteBuffer bb = ByteBuffer.allocate(8);
		in.read(bb.array(), 0, bb.limit());
		return bb;
	}

	/**
	 * Write a response package to the client
	 * @param responsePackage
	 */
	protected boolean writeResultPackage(final NetworkResponsePackage responsePackage) {
		
		final byte[] outputData = responsePackage.getByteArray();
		
		try {
			out.write(outputData, 0, outputData.length);
			out.flush();
			return true;
		} catch (IOException e) {
			logger.warn("Unable to write result package", e);
		}

		return false;
	}
	
	@Override
	public void run() {
		try {
			logger.debug("Handling new connection from: " + clientSocket.getInetAddress());

			while(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				handleNextPackage();
			}
			
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			logger.info("Closing connection to: " + clientSocket.getInetAddress());
		} catch (IOException e) {
			// Ignore exception on closing sockets
			if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
				logger.error("Unable to read data from socket (state: " + connectionState + ")", e);
			}
		}
		
		try {
			clientSocket.close();
		} catch (IOException e) {
			// Ignore close exception
		}
	}
	
	/**
	 * Handle the delete table call
	 * @param packageSequence 
	 * @return
	 */
	protected boolean handleDeleteTable(final ByteBuffer packageHeader, final short packageSequence) {
		
		final ByteBuffer encodedPackage = readFullPackage(packageHeader);
		
		// Try to read the full package
		if(encodedPackage == null) {
			return false;
		}
		
		final DeleteTableRequest resultPackage = DeleteTableRequest.decodeTuple(encodedPackage.array());
		logger.info("Got delete call for table: " + resultPackage.getTable());
		
		// Propergate the call to the storage manager
		StorageInterface.deleteTable(resultPackage.getTable());
		
		writeResultPackage(new SuccessResponse(packageSequence));
		
		return true;
	}

	/**
	 * Read the full package. The total length of the package is read from the package header.
	 * @param packageHeader
	 * @return
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader) {
		int bodyLength = NetworkPackageDecoder.getBodyLengthFromRequestPackage(packageHeader);
		final ByteBuffer encodedPackage = ByteBuffer.allocate(packageHeader.limit() + bodyLength);
		encodedPackage.put(packageHeader.array());
		
		try {
			//System.out.println("Trying to read: " + bodyLength + " avail " + in.available());
			in.read(encodedPackage.array(), encodedPackage.position(), bodyLength);
		} catch (IOException e) {
			logger.error("IO-Exception while reading package", e);
			return null;
		}
		
		return encodedPackage;
	}

	/**
	 * Handle the next request package
	 * @throws IOException
	 */
	protected void handleNextPackage() throws IOException {
		final ByteBuffer bb = readNextPackageHeader();
		final short packageSequence = NetworkPackageDecoder.getRequestIDFromRequestPackage(bb);
		final byte packageType = NetworkPackageDecoder.getPackageTypeFromRequest(bb);
		
		switch (packageType) {
			case NetworkConst.REQUEST_TYPE_DISCONNECT:
				logger.info("Got disconnect package, closing connection");
				writeResultPackage(new SuccessResponse(packageSequence));
				connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				break;
				
			case NetworkConst.REQUEST_TYPE_DELETE_TABLE:
				logger.info("Got delete table package");
				boolean result = handleDeleteTable(bb, packageSequence);
				if(! result) {
					connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				}
				break;

			default:
				logger.warn("Got unknown package type, closing connection: " + packageType);
				connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				break;
		}
	}	
}