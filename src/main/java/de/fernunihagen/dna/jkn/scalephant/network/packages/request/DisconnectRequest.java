package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;

public class DisconnectRequest implements NetworkRequestPackage {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DisconnectRequest.class);

	
	@Override
	public byte[] getByteArray(final short sequenceNumber) {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();

		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			// Body is empty
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
	public static DisconnectRequest decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(bb, NetworkConst.REQUEST_TYPE_DISCONNECT);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		if(bb.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + bb.remaining());
		}
		
		return new DisconnectRequest();
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DISCONNECT;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DisconnectRequest) {
			return true;
		}
		
		return false;
	}
}
