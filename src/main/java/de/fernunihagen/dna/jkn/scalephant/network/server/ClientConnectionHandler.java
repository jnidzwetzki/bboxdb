package de.fernunihagen.dna.jkn.scalephant.network.server;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.response.SuccessResponse;

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
	protected BufferedReader in;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ClientConnectionHandler.class);
	

	public ClientConnectionHandler(final Socket clientSocket) {
		this.clientSocket = clientSocket;
		
		try {
			out = new BufferedOutputStream(clientSocket.getOutputStream());
		} catch (IOException e) {
			out = null;
			logger.error("Exception while creating output stream", e);
		}
		
		try {
			in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		} catch (IOException e) {
			in = null;
			logger.error("Exception while creating input stream", e);
		}
	}

	/**
	 * Read the next pakage header from the socket
	 * @return The package header, wrapped in a ByteBuffer
	 * @throws IOException
	 */
	protected ByteBuffer readNextPackageHeader() throws IOException {
		final ByteBuffer bb = ByteBuffer.allocate(8);
		in.read(bb.asCharBuffer().array(), 0, bb.limit());
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
			
			boolean readNewData = true;
			while(readNewData) {
				final ByteBuffer bb = readNextPackageHeader();
				final short packageSequence = NetworkPackageDecoder.getRequestIDFromRequestPackage(bb);
				final byte packageType = NetworkPackageDecoder.getPackageTypeFromRequest(bb);
				
				if(packageType == NetworkConst.REQUEST_TYPE_DISCONNECT) {
					writeResultPackage(new SuccessResponse(packageSequence));
					readNewData = false;
					continue;
				}
			}
			
			logger.info("Closing connection to: " + clientSocket.getInetAddress());
		} catch (IOException e) {
			// Ignore close exception
		}
		
		try {
			clientSocket.close();
		} catch (IOException e) {
			// Ignore close exception
		}
	}	
}