package de.fernunihagen.dna.jkn.scalephant;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class ScalephantConfigurationManager {

	/**
	 * The configuration of the software
	 */
	protected static ScalephantConfiguration scalephantConfiguration;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ScalephantConfigurationManager.class);

	
	/**
	 * Get the configuration of the scalephant
	 * @return
	 */
	public static synchronized ScalephantConfiguration getConfiguration() {
		
		if(scalephantConfiguration == null) {
			loadConfiguration();
		}
		
		return scalephantConfiguration;
	}

	/**
	 * Load the configuration of the scalephant from the classpath
	 */
	public static void loadConfiguration() {
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader(); 
		final URL inputFile = classLoader.getResource(Const.CONFIG_FILE);
		
		if(inputFile == null) {
			scalephantConfiguration = new ScalephantConfiguration();
			logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			logger.warn("!! No configuration file found, using default values !!");
			logger.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
			return;
		}
		
		try (final InputStream inputStream = inputFile.openStream()) {
			logger.info("Loading configuration from: " + inputFile); 
			
	        final Yaml yaml = new Yaml(); 
	        scalephantConfiguration = yaml.loadAs(inputStream, ScalephantConfiguration.class);
		} catch(IOException e) {
			logger.error("Exception while loading configuration", e);
			throw new RuntimeException(e);
		}
	}
	
}
