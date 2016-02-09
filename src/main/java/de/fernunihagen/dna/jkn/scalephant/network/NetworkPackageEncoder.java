package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkPackageEncoder {
	
	/**
	 * The sequence number generator 
	 */
	protected final SequenceNumberGenerator sequenceNumberGenerator;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkPackageEncoder.class);
	
	
	public NetworkPackageEncoder(final SequenceNumberGenerator sequenceNumberGenerator) {
		this.sequenceNumberGenerator = sequenceNumberGenerator;
	}
	
	/**
	 * Append the request package header to the output stream
	 * @param packageType
	 * @param bos
	 */
	protected void appendRequestPackageHeader(final byte packageType, final ByteArrayOutputStream bos) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(4);
		byteBuffer.order(NetworkConst.NETWORK_BYTEORDER);
		byteBuffer.put(NetworkConst.PROTOCOL_VERSION);
		byteBuffer.put(packageType);
		byteBuffer.putShort(sequenceNumberGenerator.getNextSequenceNummber());

		try {
			bos.write(byteBuffer.array());
		} catch (IOException e) {
			logger.error("Exception while writing", e);
		}
	}
	
	/**
	 * Append the response package header to the output stream
	 * @param requestId
	 * @param bos
	 */
	protected void appendResponsePackageHeader(final short requestId, final ByteArrayOutputStream bos) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(3);
		byteBuffer.order(NetworkConst.NETWORK_BYTEORDER);
		byteBuffer.put(NetworkConst.PROTOCOL_VERSION);
		byteBuffer.putShort(requestId);

		try {
			bos.write(byteBuffer.array());
		} catch (IOException e) {
			logger.error("Exception while writing", e);
		}
	}
	
	/**
	 * Return a byte array output stream for request packages that contains 
	 * the header of the package
	 * @param packageType
	 * @return
	 */
	public ByteArrayOutputStream getOutputStreamForRequestPackage(final byte packageType) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		// Append the frame header to the package
		appendRequestPackageHeader(packageType, bos);
		
		return bos;
	}
	
	/**
	 * Return a bate array output stream for response packages that contains
	 * the header of the package
	 * @param packageType
	 * @return
	 */
	public ByteArrayOutputStream getOutputStreamForResponsePackage(final short requestId) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		// Append the frame header to the package
		appendResponsePackageHeader(requestId, bos);
		
		return bos;
	}
}
