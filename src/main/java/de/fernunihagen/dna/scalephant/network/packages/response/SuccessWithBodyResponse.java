package de.fernunihagen.dna.scalephant.network.packages.response;

import java.nio.ByteBuffer;

import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;

public class SuccessWithBodyResponse extends AbstractBodyResponse {
	
	public SuccessWithBodyResponse(final short sequenceNumber, final String body) {
		super(sequenceNumber, body);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_SUCCESS_WITH_BODY;
	}

	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeError 
	 */
	public static SuccessWithBodyResponse decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final String body = decodeMessage(encodedPackage);
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
		
		return new SuccessWithBodyResponse(requestId, body);
	}

}
