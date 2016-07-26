package de.fernunihagen.dna.jkn.scalephant.distribution.regionsplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.jkn.scalephant.ScalephantConfigurationManager;

public class RegionSplitHelper {
	
	/**
	 * The cached instance of the factory
	 */
	protected static RegionSplitFactory cachedInstance = null;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RegionSplitHelper.class);

	/**
	 * Get an instance of the configured factory
	 * @return
	 */
	public static RegionSplitFactory getFactoryInstance() {
		
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
			
			if(! (factoryObject instanceof RegionSplitFactory)) {
				throw new ClassNotFoundException(factoryClass + " is not a instance of RegionSplitFactory");
			}
			
			cachedInstance = (RegionSplitFactory) factoryObject;
			
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
