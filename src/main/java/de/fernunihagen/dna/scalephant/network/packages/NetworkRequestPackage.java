package de.fernunihagen.dna.scalephant.network.packages;

import java.io.OutputStream;



public interface NetworkRequestPackage extends NetworkPackage {

	/**
	 * Encode the package
	 * @param outputStream 
	 */
	public abstract void writeToOutputStream(final short sequenceNumber, OutputStream outputStream);
	
}
