package de.fernunihagen.dna.jkn.scalephant.distribution.nameprefix;

import java.util.HashMap;
import java.util.Map;

import de.fernunihagen.dna.jkn.scalephant.distribution.DistributionGroupName;

public class NameprefixInstanceManager {
	
	/**
	 * The local mappings for a distribution group
	 */
	protected final static Map<DistributionGroupName, NameprefixMapper> instances;
	
	static {
		instances = new HashMap<DistributionGroupName, NameprefixMapper>();
	}
	
	/**
	 * Get the instance 
	 * @param distributionGroupName
	 */
	public synchronized NameprefixMapper getInstance(final DistributionGroupName distributionGroupName) {
		if(! instances.containsKey(distributionGroupName)) {
			instances.put(distributionGroupName, new NameprefixMapper());
		}
		
		return instances.get(distributionGroupName);
	}
}
