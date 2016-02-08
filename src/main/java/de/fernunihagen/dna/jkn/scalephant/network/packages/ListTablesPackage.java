package de.fernunihagen.dna.jkn.scalephant.network.packages;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.SequenceNumberGenerator;

public class ListTablesPackage implements NetworkPackage {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ListTablesPackage.class);

	
	@Override
	public byte[] getByteArray(SequenceNumberGenerator sequenceNumberGenerator) {
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder(sequenceNumberGenerator);

		final ByteArrayOutputStream bos = networkPackageEncoder.getByteOutputStream(getPackageType());
		
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
	public static ListTablesPackage decodeTuple(final byte encodedPackage[]) {
		final ByteBuffer bb = NetworkPackageDecoder.encapsulateBytes(encodedPackage);
		NetworkPackageDecoder.validatePackageHeader(bb, NetworkConst.REQUEST_TYPE_LIST_TABLES);
		
		if(bb.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + bb.remaining());
		}
		
		return new ListTablesPackage();
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_LIST_TABLES;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof ListTablesPackage) {
			return true;
		}
		
		return false;
	}
}
