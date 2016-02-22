package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.ClientOperationFuture;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConnectionState;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.SequenceNumberGenerator;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTableRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DeleteTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.DisconnectRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.InsertTupleRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.ListTablesRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.request.QueryKeyRequest;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.AbstractBodyResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.ErrorWithBodyResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.ListTablesResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.TupleResponse;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.SuccessWithBodyResponse;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class ScalephantClient {

	/**
	 * The sequence number generator
	 */
	protected final SequenceNumberGenerator sequenceNumberGenerator;
	
	/**
	 * The hostname of the server
	 */
	protected final String serverHostname;
	
	/**
	 * The port of the server
	 */
	protected int serverPort = NetworkConst.NETWORK_PORT;
	
	/**
	 * The socket of the connection
	 */
	protected Socket clientSocket = null;

	/**
	 * The input stream of the socket
	 */
	protected BufferedInputStream inputStream;

	/**
	 * The output stream of the socket
	 */
	protected BufferedOutputStream outputStream;
	
	/**
	 * The pending calls
	 */
	protected final Map<Short, ClientOperationFuture> pendingCalls = new HashMap<Short, ClientOperationFuture>();

	/**
	 * The server response reader
	 */
	protected ServerResponseReader serverResponseReader;
	
	/**
	 * The server response reader thread
	 */
	protected Thread serverResponseReaderThread;
	
	/**
	 * The connection state
	 */
	protected volatile NetworkConnectionState connectionState;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantClient.class);
	

	public ScalephantClient(final String serverHostname) {
		super();
		this.serverHostname = serverHostname;
		this.sequenceNumberGenerator = new SequenceNumberGenerator();
		this.connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
	}

	/**
	 * Connect to the server
	 * @return true or false, depending on the connection state
	 */
	public boolean connect() {
		
		if(clientSocket != null) {
			logger.warn("Connect() called on an active connection, ignoring");
			return true;
		}
		
		logger.info("Connecting to server: " + serverHostname + " on port " + serverPort);
		
		try {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPENING;
			clientSocket = new Socket(serverHostname, serverPort);
			
			inputStream = new BufferedInputStream(clientSocket.getInputStream());
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());
			pendingCalls.clear();
			
			// Start up the resonse reader
			serverResponseReader = new ServerResponseReader();
			serverResponseReaderThread = new Thread(serverResponseReader);
			serverResponseReaderThread.setName("Server response reader for " + serverHostname + " / " + serverPort);
			serverResponseReaderThread.start();
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_OPEN;
		} catch (Exception e) {
			logger.error("Got an exception while connecting to server", e);
			clientSocket = null;
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
			return false;
		} 
		
		return true;
	}
	
	/**
	 * Disconnect from the server
	 */
	public boolean disconnect() {
		
		logger.info("Disconnecting from server: " + serverHostname + " port " + serverPort);
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSING;
		sendPackageToServer(new DisconnectRequest(), new ClientOperationFuture());

		// Wait for all pending calles to settle
		synchronized (pendingCalls) {
			while(! pendingCalls.keySet().isEmpty()) {
				try {
					logger.info("Waiting for pending requests to settle");
					pendingCalls.wait();
					logger.info("All requests are settled");
				} catch (InterruptedException e) {
					return true; // Thread was canceled
				}
			}
		}
		
		if(clientSocket != null) {
			try {
				clientSocket.close();
			} catch (IOException e) {
				// Ignore exception on socket close
			}
			clientSocket = null;
		}
		
		logger.info("Disconnected from server");
		connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED;
		
		return true;
	}
	
	/**
	 * Delete a table on the scalephant server
	 * @param table
	 * @return
	 */
	public ClientOperationFuture deleteTable(final String table) {
		
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("deleteTable called, but connection not ready: " + connectionState);
			operationFuture.setFailedState();
			return operationFuture;
		}
		
		sendPackageToServer(new DeleteTableRequest(table), operationFuture);
		
		return operationFuture;
	}
	
	/**
	 * Insert a new tuple into the given table
	 * @param tuple
	 * @param table
	 * @return
	 */
	public ClientOperationFuture insertTuple(final String table, final Tuple tuple) {
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("insertTuple called, but connection not ready: " + connectionState);
			operationFuture.setFailedState();
			return operationFuture;
		}
		
		sendPackageToServer(new InsertTupleRequest(table, tuple), operationFuture);
		
		return operationFuture;
	}
	
	/**
	 * Delete the given key from a table
	 * @param table
	 * @param key
	 * @return
	 */
	public ClientOperationFuture deleteTuple(final String table, final String key) {
		final ClientOperationFuture operationFuture = new ClientOperationFuture();

		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("deleteTuple called, but connection not ready: " + connectionState);
			operationFuture.setFailedState();
			return operationFuture;
		}
		
		sendPackageToServer(new DeleteTupleRequest(table, key), operationFuture);
		
		return operationFuture;
	}
	
	/**
	 * List the existing tables
	 * @return
	 */
	public ClientOperationFuture listTables() {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("listTables called, but connection not ready: " + connectionState);
			return null;
		}

		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new ListTablesRequest(), future);

		return future;
	}
	
	/**
	 * Query a given table for a specific key
	 * @param table
	 * @param key
	 * @return
	 */
	public ClientOperationFuture queryKey(final String table, final String key) {
		if(connectionState != NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
			logger.warn("queryKey called, but connection not ready: " + connectionState);
			return null;
		}

		final ClientOperationFuture future = new ClientOperationFuture();
		sendPackageToServer(new QueryKeyRequest(table, key), future);

		return future;
	}
	
	/**
	 * Set an alternative server port
	 * @param serverPort
	 */
	public void setPort(final int serverPort) {
		this.serverPort = serverPort;
	}
	
	/**
	 * Is the client connected?
	 * @return
	 */
	public boolean isConnected() {
		if(clientSocket != null) {
			return ! clientSocket.isClosed();
		}
		
		return false;
	}
	
	/**
	 * Returns the state of the connection
	 * @return
	 */
	public NetworkConnectionState getConnectionState() {
		return connectionState;
	}

	/**
	 * Send a request package to the server
	 * @param responsePackage
	 * @return
	 * @throws IOException 
	 */
	protected short sendPackageToServer(final NetworkRequestPackage requestPackage, final ClientOperationFuture future) {
		final short sequenceNumber = sequenceNumberGenerator.getNextSequenceNummber();
		final byte[] output = requestPackage.getByteArray(sequenceNumber);

		future.setRequestId(sequenceNumber);

		try {
			outputStream.write(output, 0, output.length);
			outputStream.flush();
			
			synchronized (pendingCalls) {
				pendingCalls.put(sequenceNumber, future);
			}
		} catch (IOException e) {
			future.setFailedState();
		}

		return sequenceNumber;
	}
	
	
	/**
	 * Read the server response packages
	 *
	 */
	class ServerResponseReader implements Runnable {
		
		/**
		 * Read the next response package header from the server
		 * @return 
		 * @throws IOException 
		 */
		protected ByteBuffer readNextResponsePackageHeader() throws IOException {
			final ByteBuffer bb = ByteBuffer.allocate(8);
			int read = inputStream.read(bb.array(), 0, bb.limit());
			
			// Read error
			if(read == -1) {
				return null;
			}
			
			return bb;
		}
		
		/**
		 * Process the next server answer
		 */
		protected boolean processNextResponsePackage() {
			try {
				final ByteBuffer bb = readNextResponsePackageHeader();
				
				if(bb == null) {
					logger.error("Read error from socket, exiting");
					return false;
				}
				
				handleResultPackage(bb);
				
			} catch (IOException e) {
				// Ignore exceptions when connection is closing
				if(connectionState == NetworkConnectionState.NETWORK_CONNECTION_OPEN) {
					logger.error("Unable to read data from server (state: " + connectionState + ")", e);
				}
			}
			
			return true;
		}

		/**
		 * Handle the next result package
		 * @param packageHeader
		 */
		protected void handleResultPackage(final ByteBuffer packageHeader) {
			final short sequenceNumber = NetworkPackageDecoder.getRequestIDFromResponsePackage(packageHeader);
			final byte packageType = NetworkPackageDecoder.getPackageTypeFromResponse(packageHeader);
			final ByteBuffer encodedPackage = readFullPackage(packageHeader);

			ClientOperationFuture pendingCall = null;
			synchronized (pendingCalls) {
				pendingCall = pendingCalls.get(Short.valueOf(sequenceNumber));
			}

			switch(packageType) {
				case NetworkConst.RESPONSE_SUCCESS:
					if(pendingCall != null) {
						pendingCall.setOperationResult(true);
					}
					break;
				case NetworkConst.RESPONSE_ERROR:
					if(pendingCall != null) {
						pendingCall.setOperationResult(false);
					}
					break;
				case NetworkConst.RESPONSE_SUCCESS_WITH_BODY:
					handleSuccessWithBody(encodedPackage, pendingCall);
					break;
				case NetworkConst.RESPONSE_ERROR_WITH_BODY:
					handleErrorWithBody(encodedPackage, pendingCall);
					break;
				case NetworkConst.RESPONSE_LIST_TABLES:
					handleListTables(encodedPackage, pendingCall);
					break;
				case NetworkConst.RESPONSE_TUPLE:
					handleSingleTuple(encodedPackage, pendingCall);
					break;
				default:
					logger.error("Unknown respose package type: " + packageType);
					if(pendingCall != null) {
						pendingCall.setFailedState();
					}
			}
			
			// Remove pending call
			synchronized (pendingCalls) {
				pendingCalls.remove(Short.valueOf(sequenceNumber));
				pendingCalls.notifyAll();
			}
		}
		
		/**
		 * Kill pending calls
		 */
		protected void handleSocketClosedUnexpected() {
			connectionState = NetworkConnectionState.NETWORK_CONNECTION_CLOSED_WITH_ERRORS;
			
			synchronized (pendingCalls) {
				if(! pendingCalls.isEmpty()) {
					logger.warn("Socket is closed unexpected, killing pending calls: " + pendingCalls.size());
				
					for(short requestId : pendingCalls.keySet()) {
						final ClientOperationFuture future = pendingCalls.get(requestId);
						future.setFailedState();
					}
					
					pendingCalls.clear();
					pendingCalls.notifyAll();
				}
			}
		}

		@Override
		public void run() {
			logger.info("Started new response reader for " + serverHostname + " / " + serverPort);
			
			while(clientSocket != null) {
				boolean result = processNextResponsePackage();
				
				if(result == false) {
					handleSocketClosedUnexpected();
					break;
				}
				
			}
			
			logger.info("Stopping new response reader for " + serverHostname + " / " + serverPort);
		}
	}

	/**
	 * Read the full package
	 * @param packageHeader
	 * @return
	 */
	protected ByteBuffer readFullPackage(final ByteBuffer packageHeader) {
		int bodyLength = NetworkPackageDecoder.getBodyLengthFromResponsePackage(packageHeader);
		final ByteBuffer encodedPackage = ByteBuffer.allocate(packageHeader.limit() + bodyLength);
		encodedPackage.put(packageHeader.array());
		
		try {
			//System.out.println("Trying to read: " + bodyLength + " avail " + inputStream.available());
			inputStream.read(encodedPackage.array(), encodedPackage.position(), bodyLength);
		} catch (IOException e) {
			logger.error("IO-Exception while reading package", e);
			return null;
		}
		
		return encodedPackage;
	}

	/**
	 * Handle a single tuple as result
	 * @param encodedPackage
	 * @param pendingCall
	 */
	protected void handleSingleTuple(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) {
		final TupleResponse singleTupleResponse = TupleResponse.decodeTuple(encodedPackage);
		pendingCall.setOperationResult(singleTupleResponse.getTuple());
	}

	/**
	 * Handle List table result
	 * @param encodedPackage
	 * @param pendingCall
	 */
	protected void handleListTables(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) {
		final ListTablesResponse tables = ListTablesResponse.decodeTuple(encodedPackage);
		pendingCall.setOperationResult(tables.getTables());
	}

	/**
	 * Handle error with body result
	 * @param encodedPackage
	 * @param pendingCall
	 */
	protected void handleErrorWithBody(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) {
		final AbstractBodyResponse result = ErrorWithBodyResponse.decodeTuple(encodedPackage);
		pendingCall.setOperationResult(result.getBody());
	}

	/**
	 * Handle success with body result
	 * @param encodedPackage
	 * @param pendingCall
	 */
	protected void handleSuccessWithBody(final ByteBuffer encodedPackage,
			final ClientOperationFuture pendingCall) {
		final SuccessWithBodyResponse result = SuccessWithBodyResponse.decodeTuple(encodedPackage);
		pendingCall.setOperationResult(result.getBody());
	}	
}
