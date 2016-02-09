package de.fernunihagen.dna.jkn.scalephant.network.packages;


public interface NetworkResponsePackage extends NetworkPackage {

	/**
	 * Encode the package
	 * @return 
	 */
	public abstract byte[] getByteArray(final short sequenceNumber);
	
}
