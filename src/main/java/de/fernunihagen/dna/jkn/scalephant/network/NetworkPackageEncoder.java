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
	 * Append the package header to the output stream
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
	 * Return a byte arry output stream that contains the header
	 * of the package
	 * @return
	 */
	public ByteArrayOutputStream getOutputStreamForRequestPackage(final byte packageType) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		// Append the frame header to the package
		appendRequestPackageHeader(packageType, bos);
		
		return bos;
	}
}
