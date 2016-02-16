package de.fernunihagen.dna.jkn.scalephant.network.packages;


public interface NetworkQueryRequestPackage extends NetworkRequestPackage {

	/**
	 * Returns the query type of the package as a byte
	 * @return
	 */
	public abstract byte getQueryType();
	
}
