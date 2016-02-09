package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkPackageEncoder {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkPackageEncoder.class);
	
	/**
	 * Append the request package header to the output stream
	 * @param sequenceNumberGenerator 
	 * @param packageType
	 * @param bos
	 */
	protected void appendRequestPackageHeader(final SequenceNumberGenerator sequenceNumberGenerator, final byte packageType, final ByteArrayOutputStream bos) {
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
	 * @param packageType 
	 * @param bos
	 */
	protected void appendResponsePackageHeader(final short requestId, final byte packageType, final ByteArrayOutputStream bos) {
		final ByteBuffer byteBuffer = ByteBuffer.allocate(4);
		byteBuffer.order(NetworkConst.NETWORK_BYTEORDER);
		byteBuffer.put(NetworkConst.PROTOCOL_VERSION);
		byteBuffer.putShort(requestId);
		byteBuffer.put(packageType);


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
	public ByteArrayOutputStream getOutputStreamForRequestPackage(final SequenceNumberGenerator sequenceNumberGenerator, final byte packageType) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		// Append the frame header to the package
		appendRequestPackageHeader(sequenceNumberGenerator, packageType, bos);
		
		return bos;
	}
	
	/**
	 * Return a bate array output stream for response packages that contains
	 * the header of the package
	 * @param packageType
	 * @return
	 */
	public ByteArrayOutputStream getOutputStreamForResponsePackage(final short requestId, final byte packageType) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		// Append the frame header to the package
		appendResponsePackageHeader(requestId, packageType, bos);
		
		return bos;
	}
}
