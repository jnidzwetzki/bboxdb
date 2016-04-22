package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;

public class ListTablesRequest implements NetworkRequestPackage {
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ListTablesRequest.class);

	
	@Override
	public void writeToOutputStream(final short sequenceNumber,
			final OutputStream outputStream) {
		
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();

		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			// Body is empty
			final ByteBuffer bodyLengthBuffer = ByteBuffer.allocate(8);
			bodyLengthBuffer.order(NetworkConst.NETWORK_BYTEORDER);
			bodyLengthBuffer.putLong(0);
			bos.write(bodyLengthBuffer.array());
			bos.close();
			
			final byte[] outputData = bos.toByteArray();
			outputStream.write(outputData, 0, outputData.length);
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
	public static ListTablesRequest decodeTuple(final ByteBuffer encodedPackage) {
		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_LIST_TABLES);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		return new ListTablesRequest();
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_LIST_TABLES;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof ListTablesRequest) {
			return true;
		}
		
		return false;
	}
}
