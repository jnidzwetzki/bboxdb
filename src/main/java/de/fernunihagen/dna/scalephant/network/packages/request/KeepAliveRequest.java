package de.fernunihagen.dna.scalephant.network.packages.request;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeader;

public class KeepAliveRequest implements NetworkRequestPackage {
	
	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) throws PackageEncodeError {

		try {
			// Write body length
			final long bodyLength = 0;
			
			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, routingHeader, 
					getPackageType(), outputStream);
		} catch (Exception e) {
			throw new PackageEncodeError("Got exception while converting package into bytes", e);
		}	
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeError 
	 */
	public static KeepAliveRequest decodeTuple(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_KEEP_ALIVE);
		
		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
		}
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeError("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new KeepAliveRequest();
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_KEEP_ALIVE;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof KeepAliveRequest) {
			return true;
		}
		
		return false;
	}
}
