package de.fernunihagen.dna.scalephant.network.packages.request;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.scalephant.tools.DataEncoderHelper;

public class DeleteDistributionGroupRequest implements NetworkRequestPackage {
	
	/**
	 * The name of the table
	 */
	protected final String distributionGroup;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DeleteDistributionGroupRequest.class);
	
	public DeleteDistributionGroupRequest(final String distributionGroup) {
		this.distributionGroup = distributionGroup;
	}
	
	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) {

		try {
			final byte[] groupBytes = distributionGroup.getBytes();
			final ByteBuffer bb = DataEncoderHelper.shortToByteBuffer((short) groupBytes.length);
			
			// Body length
			final long bodyLength = bb.capacity() + groupBytes.length;
			
			// Unrouted package
			final RoutingHeader routingHeader = new RoutingHeader(false);
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, 
					routingHeader, getPackageType(), outputStream);

			// Write body
			outputStream.write(bb.array());
			outputStream.write(groupBytes);			
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
		}
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static DeleteDistributionGroupRequest decodeTuple(final ByteBuffer encodedPackage) {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		final short groupLength = encodedPackage.getShort();
		
		final byte[] groupBytes = new byte[groupLength];
		encodedPackage.get(groupBytes, 0, groupBytes.length);
		final String distributionGroup = new String(groupBytes);
		
		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after decoding: " + encodedPackage.remaining());
		}
		
		return new DeleteDistributionGroupRequest(distributionGroup);
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_DELETE_DISTRIBUTION_GROUP;
	}

	public String getDistributionGroup() {
		return distributionGroup;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((distributionGroup == null) ? 0 : distributionGroup.hashCode());
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
		DeleteDistributionGroupRequest other = (DeleteDistributionGroupRequest) obj;
		if (distributionGroup == null) {
			if (other.distributionGroup != null)
				return false;
		} else if (!distributionGroup.equals(other.distributionGroup))
			return false;
		return true;
	}

}
