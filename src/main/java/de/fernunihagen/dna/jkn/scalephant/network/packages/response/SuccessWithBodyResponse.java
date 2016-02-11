package de.fernunihagen.dna.jkn.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkResponsePackage;

public class SuccessWithBodyResponse extends NetworkResponsePackage {
	
	/**
	 * The result body
	 */
	protected final String body;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SuccessWithBodyResponse.class);

	public SuccessWithBodyResponse(final short sequenceNumber, final String body) {
		super(sequenceNumber);
		this.body = body;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_SUCCESS_WITH_BODY;
	}

	@Override
	public byte[] getByteArray() {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());
		
		try {
			final byte[] bodyBytes = body.getBytes();
			final ByteBuffer bb = ByteBuffer.allocate(2);
			bb.order(NetworkConst.NETWORK_BYTEORDER);
			bb.putShort((short) bodyBytes.length);

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
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static SuccessWithBodyResponse decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		
		NetworkPackageDecoder.validateResponsePackageHeader(bb, NetworkConst.RESPONSE_SUCCESS_WITH_BODY);

		short bodyLength = bb.getShort();
		
		final byte[] bodyBytes = new byte[bodyLength];
		bb.get(bodyBytes, 0, bodyBytes.length);
		final String body = new String(bodyBytes);
		
		if(bb.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + bb.remaining());
		}
		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(bb);
		
		return new SuccessWithBodyResponse(requestId, body);
	}

}
