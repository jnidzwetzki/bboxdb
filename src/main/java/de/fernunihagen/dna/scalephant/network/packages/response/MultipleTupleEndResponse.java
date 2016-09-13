package de.fernunihagen.dna.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.fernunihagen.dna.scalephant.Const;
import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;

public class MultipleTupleEndResponse extends NetworkResponsePackage {

	public MultipleTupleEndResponse(final short sequenceNumber) {
		super(sequenceNumber);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END;
	}

	@Override
	public byte[] getByteArray() throws PackageEncodeError {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
	
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());
		
		try {
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(Const.APPLICATION_BYTE_ORDER);
			bodyLengthBuffer.putLong(0);
			bos.write(bodyLengthBuffer.array());
			
			bos.close();
		} catch (IOException e) {
			throw new PackageEncodeError("Got exception while converting package into bytes", e);
		}
	
		return bos.toByteArray();
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeError 
	 */
	public static MultipleTupleEndResponse decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeError {
		
		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_MULTIPLE_TUPLE_END);
		
		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeError("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);
		
		return new MultipleTupleEndResponse(requestId);
	}

}
