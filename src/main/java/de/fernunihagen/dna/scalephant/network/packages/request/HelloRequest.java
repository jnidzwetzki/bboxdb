package de.fernunihagen.dna.scalephant.network.packages.request;

import java.io.OutputStream;
import java.nio.ByteBuffer;

import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.capabilities.PeerCapabilities;
import de.fernunihagen.dna.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.scalephant.tools.DataEncoderHelper;

public class HelloRequest implements NetworkRequestPackage {
	
	/**
	 * The supported protocol version
	 */
	protected final int protocolVersion;
	
	/**
	 * The peer capabilities (e.g. compression)
	 */
	protected final PeerCapabilities peerCapabilities;
	
	public HelloRequest(final int protocolVersion, final PeerCapabilities peerCapabilities) {
		this.protocolVersion = protocolVersion;
		this.peerCapabilities = peerCapabilities;
	}
	
	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) throws PackageEncodeError {

		try {
			final ByteBuffer bb = DataEncoderHelper.intToByteBuffer(protocolVersion);
			final byte[] peerCapabilitiesBytes = peerCapabilities.toByteArray();
			
			// Body length
			final long bodyLength = bb.capacity() + peerCapabilitiesBytes.length;
			
			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, routingHeader, 
					getPackageType(), outputStream);
			
			outputStream.write(bb.array());
			outputStream.write(peerCapabilitiesBytes);
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
	public static HelloRequest decodeRequest(final ByteBuffer encodedPackage) throws PackageEncodeError {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_HELLO);
		
		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
		}
		
		final int protocolVersion = encodedPackage.getInt();
		final byte[] capabilityBytes = new byte[PeerCapabilities.CAPABILITY_BYTES];
		encodedPackage.get(capabilityBytes, 0, capabilityBytes.length);

		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeError("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		final PeerCapabilities peerCapabilities = new PeerCapabilities(capabilityBytes);
		
		return new HelloRequest(protocolVersion, peerCapabilities);
	}
	
	/**
	 * Get the capabilities
	 * @return
	 */
	public PeerCapabilities getPeerCapabilities() {
		return peerCapabilities;
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_HELLO;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((peerCapabilities == null) ? 0 : peerCapabilities.hashCode());
		result = prime * result + protocolVersion;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		HelloRequest other = (HelloRequest) obj;
		if (peerCapabilities == null) {
			if (other.peerCapabilities != null)
				return false;
		} else if (!peerCapabilities.equals(other.peerCapabilities))
			return false;
		if (protocolVersion != other.protocolVersion)
			return false;
		return true;
	}

}
