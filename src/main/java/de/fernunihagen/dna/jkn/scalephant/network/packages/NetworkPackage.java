package de.fernunihagen.dna.jkn.scalephant.network.packages;

import de.fernunihagen.dna.jkn.scalephant.network.SequenceNumberGenerator;

public interface NetworkPackage {

	/**
	 * Encode the package
	 * @return 
	 */
	public abstract byte[] getByteArray(final SequenceNumberGenerator sequenceNumberGenerator);
	
	/**
	 * Returns the type of the package as a byte
	 * @return
	 */
	public abstract byte getPackageType();

}
