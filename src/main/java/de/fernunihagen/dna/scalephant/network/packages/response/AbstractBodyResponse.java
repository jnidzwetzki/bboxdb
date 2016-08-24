package de.fernunihagen.dna.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.Const;
import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkResponsePackage;

public abstract class AbstractBodyResponse extends NetworkResponsePackage {

	/**
	 * The result body
	 */
	protected final String body;
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(AbstractBodyResponse.class);

	public AbstractBodyResponse(final short sequenceNumber, final String body) {
		super(sequenceNumber);
		this.body = body;
	}

	@Override
	public byte[] getByteArray() {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] bodyBytes = body.getBytes();
			final ByteBuffer bb = ByteBuffer.allocate(2);
			bb.order(Const.APPLICATION_BYTE_ORDER);
			bb.putShort((short) bodyBytes.length);
			
			// Write body length
			final long bodyLength = bb.capacity() + bodyBytes.length;
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(Const.APPLICATION_BYTE_ORDER);
			bodyLengthBuffer.putLong(bodyLength);
			bos.write(bodyLengthBuffer.array());
	
			// Write body
			bos.write(bb.array());
			bos.write(bodyBytes);
			
			bos.close();
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
			return null;
		}
	
		return bos.toByteArray();
	}
	

	/**
	 * Decode the message of the answer
	 * @param bb
	 * @return
	 */
	protected static String decodeMessage(final ByteBuffer bb) {
		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(bb, NetworkConst.RESPONSE_ERROR_WITH_BODY);

		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		final short bodyLength = bb.getShort();
		
		final byte[] bodyBytes = new byte[bodyLength];
		bb.get(bodyBytes, 0, bodyBytes.length);
		final String body = new String(bodyBytes);
		
		if(bb.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + bb.remaining());
		}
		
		return body;
	}

	/**
	 * Get the message string
	 * @return
	 */
	public String getBody() {
		return body;
	}

}