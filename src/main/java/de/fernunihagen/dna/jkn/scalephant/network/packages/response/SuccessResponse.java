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

public class SuccessResponse extends NetworkResponsePackage {

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SuccessResponse.class);

	public SuccessResponse(final short sequenceNumber) {
		super(sequenceNumber);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_SUCCESS;
	}

	@Override
	public byte[] getByteArray() {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());
		
		try {
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(4);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putInt(0);
			bos.write(bodyLengthBuffer.array());
			
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
	public static SuccessResponse decodePackage(final ByteBuffer encodedPackage) {
		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_SUCCESS);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
		
		return new SuccessResponse(requestId);
	}

}
