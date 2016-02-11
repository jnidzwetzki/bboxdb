package de.fernunihagen.dna.jkn.scalephant.network.packages;


public interface NetworkRequestPackage extends NetworkPackage {

	/**
	 * Encode the package
	 * @return 
	 */
	public abstract byte[] getByteArray(final short sequenceNumber);
	
}
