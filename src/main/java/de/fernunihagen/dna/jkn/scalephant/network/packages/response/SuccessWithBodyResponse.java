package de.fernunihagen.dna.jkn.scalephant.network.packages.response;

import java.nio.ByteBuffer;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;

public class SuccessWithBodyResponse extends AbstractBodyResponse {
	
	public SuccessWithBodyResponse(final short sequenceNumber, final String body) {
		super(sequenceNumber, body);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_SUCCESS_WITH_BODY;
	}

	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static SuccessWithBodyResponse decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		final String body = decodeMessage(bb);
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(bb);
		
		return new SuccessWithBodyResponse(requestId, body);
	}

}
