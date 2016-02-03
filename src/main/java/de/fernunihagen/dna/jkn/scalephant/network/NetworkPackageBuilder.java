package de.fernunihagen.dna.jkn.scalephant.network;

import java.io.ByteArrayOutputStream;

public class NetworkPackageBuilder {
	
	/**
	 * The sequence number generator 
	 */
	protected final SequenceNumberGenerator sequenceNumberGenerator;
	
	public NetworkPackageBuilder(final SequenceNumberGenerator sequenceNumberGenerator) {
		this.sequenceNumberGenerator = sequenceNumberGenerator;
	}
	
	/**
	 * Append the package header to the output stream
	 * @param bos
	 */
	protected void appendPackageHeader(final short packageType, final ByteArrayOutputStream bos) {
		bos.write(NetworkConst.PROTOCOL_VERSION);
		bos.write(packageType);
		bos.write(sequenceNumberGenerator.getNextSequenceNummber());
	}
	
	/**
	 * Return a byte arry output stream that contains the header
	 * of the package
	 * @return 
	 * 
	 */
	protected ByteArrayOutputStream getByteOutputStream(final short packageType) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		
		// Append the frame header to the package
		appendPackageHeader(packageType, bos);
		
		return bos;
	}
}
