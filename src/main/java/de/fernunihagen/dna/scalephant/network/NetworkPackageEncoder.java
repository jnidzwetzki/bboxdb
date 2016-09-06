package de.fernunihagen.dna.scalephant.network;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.Const;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeaderParser;

public class NetworkPackageEncoder {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(NetworkPackageEncoder.class);
	
	/**
	 * Append the request package header to the output stream
	 * @param routingHeader 
	 * @param bodyLength 
	 * @param sequenceNumberGenerator 
	 * @param packageType
	 * @param bos
	 */
	public static void appendRequestPackageHeader(final short sequenceNumber, final long bodyLength, 
			final RoutingHeader routingHeader, final byte packageType, final OutputStream bos) {
		
		final ByteBuffer byteBuffer = ByteBuffer.allocate(12);
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
		byteBuffer.putShort(packageType);
		byteBuffer.putShort(sequenceNumber);
		byteBuffer.putLong(bodyLength);

		try {
			bos.write(byteBuffer.array());
			
			// Write routing header
			final byte[] routingHeaderBytes = RoutingHeaderParser.encodeHeader(routingHeader);
			bos.write(routingHeaderBytes);
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
		byteBuffer.order(Const.APPLICATION_BYTE_ORDER);
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
	 * Return a byte array output stream for response packages that contains
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
