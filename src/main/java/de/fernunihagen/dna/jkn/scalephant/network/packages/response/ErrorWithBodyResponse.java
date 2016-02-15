package de.fernunihagen.dna.jkn.scalephant.network.packages.response;

import java.nio.ByteBuffer;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;

public class ErrorWithBodyResponse extends AbstractBodyResponse {
	
	public ErrorWithBodyResponse(final short sequenceNumber, final String body) {
		super(sequenceNumber, body);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_ERROR_WITH_BODY;
	}

	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static ErrorWithBodyResponse decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final String body = decodeMessage(bb);
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(bb);
		
		return new ErrorWithBodyResponse(requestId, body);
	}
}
