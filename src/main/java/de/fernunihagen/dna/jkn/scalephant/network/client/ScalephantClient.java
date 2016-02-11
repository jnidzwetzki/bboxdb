package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.SequenceNumberGenerator;

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
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantClient.class);
	

	public ScalephantClient(final String serverHostname) {
		super();
		this.serverHostname = serverHostname;
		this.sequenceNumberGenerator = new SequenceNumberGenerator();
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
			clientSocket = new Socket(serverHostname, serverPort);
			
			inputStream = new BufferedInputStream(clientSocket.getInputStream());
			outputStream = new BufferedOutputStream(clientSocket.getOutputStream());
		} catch (Exception e) {
			logger.error("Got an exception while connecting to server", e);
			clientSocket = null;
			return false;
		} 
		
		return true;
	}
	
	/**
	 * Disconnect from the server
	 */
	public void disconnect() {
		if(clientSocket != null) {
			try {
				clientSocket.close();
			} catch (IOException e) {
				// Ignore exception on socket close
			}
			clientSocket = null;
		}
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
	
}
