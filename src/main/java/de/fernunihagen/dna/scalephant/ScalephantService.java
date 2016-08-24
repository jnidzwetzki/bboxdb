package de.fernunihagen.dna.scalephant;

public interface ScalephantService {
	
	/**
	 * Init the service
	 */
	public void init();
	
	/**
	 * Shutdown the service
	 */
	public void shutdown();
	
	/**
	 * Get the name of the service
	 * @return
	 */
	public String getServicename();
}
