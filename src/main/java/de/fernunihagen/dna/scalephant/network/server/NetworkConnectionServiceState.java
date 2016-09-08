package de.fernunihagen.dna.scalephant.network.server;

public class NetworkConnectionServiceState {

	/**
	 * Readonly mode
	 */
	protected boolean readonly = true;
	
	/**
	 * Set the readonly mode
	 * @param readonly
	 */
	public void setReadonly(final boolean readonly) {
		this.readonly = readonly;
	}

	/**
	 * Get the readonly mode
	 * @return
	 */
	public boolean isReadonly() {
		return readonly;
	}

}
