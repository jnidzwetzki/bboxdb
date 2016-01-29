package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.IOException;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConnectionHandler implements Runnable {
	
	/**
	 * The client socket
	 */
	protected final Socket clientSocket;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientConnectionHandler.class);
	

	public ClientConnectionHandler(final Socket clientSocket) {
		this.clientSocket = clientSocket;
	}


	@Override
	public void run() {
		logger.debug("Handling new connection from: " + clientSocket.getInetAddress());
		
		
		logger.info("Closing connection to: " + clientSocket.getInetAddress());
		try {
			clientSocket.close();
		} catch (IOException e) {
			// Ignore close exception
		}
	}	
}