package de.fernunihagen.dna.scalephant.performance.osm;

import de.fernunihagen.dna.scalephant.performance.osm.util.GeometricalStructure;

public interface OSMStructureCallback {

	/**
	 * Process the given geometrical structure
	 * @param geometricalStructure
	 */
	public void processStructure(final GeometricalStructure geometricalStructure);
	
}
