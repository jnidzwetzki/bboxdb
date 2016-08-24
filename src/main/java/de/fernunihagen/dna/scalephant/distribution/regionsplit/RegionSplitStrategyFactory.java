package de.fernunihagen.dna.scalephant.distribution.regionsplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;

public class RegionSplitStrategyFactory {
	
	/**
	 * The cached instance of the factory
	 */
	protected static RegionSplitStrategy cachedInstance = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RegionSplitStrategyFactory.class);

	/**
	 * Get an instance of the configured factory
	 * @return
	 */
	public static RegionSplitStrategy getInstance() {
		
		if(cachedInstance != null) {
			return cachedInstance;
		}
		
		final ScalephantConfiguration configuration = ScalephantConfigurationManager.getConfiguration();
		final String factoryClass = configuration.getRegionSplitStrategy();
		
		if("none".equals(factoryClass)) {
			return null;
		}
		
		// Instance the classname
		try {
			final Class<?> classObject = Class.forName(factoryClass);
			
			if(classObject == null) {
				throw new ClassNotFoundException("Unable to locate class: " + factoryClass);
			}
			
			final Object factoryObject = classObject.newInstance();
			
			if(! (factoryObject instanceof RegionSplitStrategy)) {
				throw new ClassNotFoundException(factoryClass + " is not a instance of RegionSplitter");
			}
			
			cachedInstance = (RegionSplitStrategy) factoryObject;
			
			return cachedInstance;
			
		} catch (Exception e) {
			logger.warn("Unable to instance class: " + factoryClass, e);
			throw new RuntimeException(e);
		} 
	}
	
	/**
	 * Clear the cached instance
	 */
	public static void clearCache() {
		cachedInstance = null;
	}
	
}
