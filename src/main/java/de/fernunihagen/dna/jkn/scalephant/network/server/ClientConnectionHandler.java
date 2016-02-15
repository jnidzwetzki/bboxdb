package de.fernunihagen.dna.jkn.scalephant.network.server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTableRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.InsertTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.ListTablesResponse;
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
	protected BufferedOutputStream outputStream;
	
	/**
	 * The input stream of the socket
	 */
	protected BufferedInputStream inputStream;
	
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
	 * Read the next pakage header from the socket
	 * @return The package header, wrapped in a ByteBuffer
	 * @throws IOException
	 */
	protected ByteBuffer readNextPackageHeader() throws IOException {
		final ByteBuffer bb = ByteBuffer.allocate(8);
		inputStream.read(bb.array(), 0, bb.limit());
		return bb;
	}

	/**
	 * Write a response package to the client
	 * @param responsePackage
	 */
	protected boolean writeResultPackage(final NetworkResponsePackage responsePackage) {
		
		final byte[] outputData = responsePackage.getByteArray();
		
		try {
			outputStream.write(outputData, 0, outputData.length);
			outputStream.flush();
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
	protected boolean handleDeleteTable(final ByteBuffer encodedPackage, final short packageSequence) {
		
		final DeleteTableRequest resultPackage = DeleteTableRequest.decodeTuple(encodedPackage.array());
		logger.info("Got delete call for table: " + resultPackage.getTable());
		
		// Propergate the call to the storage manager
		StorageInterface.deleteTable(resultPackage.getTable());
		
		writeResultPackage(new SuccessResponse(packageSequence));
		
		return true;
	}
	
	/**
	 * Handle query package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleQuery(final ByteBuffer encodedPackage, final short packageSequence) {
		writeResultPackage(new SuccessResponse(packageSequence));

		return true;
	}

	/**
	 * Handle Insert tuple package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleInsertTuple(final ByteBuffer encodedPackage, final short packageSequence) {
		writeResultPackage(new SuccessResponse(packageSequence));
		final InsertTupleRequest insertTupleRequest = InsertTupleRequest.decodeTuple(encodedPackage.array());
		
		return true;
	}

	/**
	 * Handle list tables package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleListTables(final ByteBuffer encodedPackage, final short packageSequence) {
		final List<String> allTables = StorageInterface.getAllTables();
		final ListTablesResponse listTablesResponse = new ListTablesResponse(packageSequence, allTables);
		writeResultPackage(listTablesResponse);
		
		return true;
	}

	/**
	 * Handle delete tuple package
	 * @param bb
	 * @param packageSequence
	 * @return
	 */
	protected boolean handleDeleteTuple(final ByteBuffer encodedPackage, final short packageSequence) {
		writeResultPackage(new SuccessResponse(packageSequence));
		final DeleteTupleRequest deleteTupleRequest = DeleteTupleRequest.decodeTuple(encodedPackage.array());
		
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
			inputStream.read(encodedPackage.array(), encodedPackage.position(), bodyLength);
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
		final ByteBuffer packageHeader = readNextPackageHeader();
		final short packageSequence = NetworkPackageDecoder.getRequestIDFromRequestPackage(packageHeader);
		final byte packageType = NetworkPackageDecoder.getPackageTypeFromRequest(packageHeader);
		final ByteBuffer encodedPackage = readFullPackage(packageHeader);

		// Try to read the full package
		if(encodedPackage == null) {
			logger.error("Unable to read full package");
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
			return;
		}
		
		boolean result = true;
		
		switch (packageType) {
			case NetworkConst.REQUEST_TYPE_DISCONNECT:
				logger.info("Got disconnect package, closing connection");
				writeResultPackage(new SuccessResponse(packageSequence));
				result = false;
				break;
				
			case NetworkConst.REQUEST_TYPE_DELETE_TABLE:
				logger.info("Got delete table package");
				result = handleDeleteTable(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_DELETE_TUPLE:
				logger.info("Got delete tuple package");
				result = handleDeleteTuple(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_LIST_TABLES:
				logger.info("Got list tables request");
				result = handleListTables(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_INSERT_TUPLE:
				logger.info("Got insert tuple request");
				result = handleInsertTuple(encodedPackage, packageSequence);
				break;
				
			case NetworkConst.REQUEST_TYPE_QUERY:
				logger.info("Got query package");
				result = handleQuery(encodedPackage, packageSequence);
				break;

			default:
				logger.warn("Got unknown package type, closing connection: " + packageType);
				connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
				break;
		}
		
		if(result == false) {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
		}	
	}
}