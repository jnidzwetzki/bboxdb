package de.fernunihagen.dna.jkn.scalephant.network.packages;

import de.fernunihagen.dna.jkn.scalephant.network.SequenceNumberGenerator;

public interface NetworkRequestPackage extends NetworkPackage {

	/**
	 * Encode the package
	 * @return 
	 */
	public abstract byte[] getByteArray(final SequenceNumberGenerator sequenceNumberGenerator);
	
}
