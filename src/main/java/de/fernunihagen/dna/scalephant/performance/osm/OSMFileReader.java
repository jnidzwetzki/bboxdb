package de.fernunihagen.dna.scalephant.performance.osm;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.osmosis.OsmosisReader;
import de.fernunihagen.dna.scalephant.network.client.ScalephantException;
import de.fernunihagen.dna.scalephant.performance.osm.filter.OSMBuildingsEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.filter.OSMMultiPointEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.filter.OSMRoadsEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.filter.OSMSinglePointEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.filter.OSMTrafficSignalEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.filter.OSMTreeEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.util.GeometricalStructure;

public class OSMFileReader implements Runnable {

	/**
	 * The single point filter
	 */
	protected final static Map<String, OSMSinglePointEntityFilter> singlePointFilter = new HashMap<String, OSMSinglePointEntityFilter>();
	
	/**
	 * The multi point filter
	 */
	protected final static Map<String, OSMMultiPointEntityFilter> multiPointFilter = new HashMap<String, OSMMultiPointEntityFilter>();
	
	/**
	 * The filename to parse
	 */
	protected final String filename;
	
	/**
	 * The type to import
	 */
	protected final String type;
	
	/**
	 * The callback for completed objects
	 */
	protected final OSMStructureCallback structureCallback;
	
	static {
		singlePointFilter.put("tree", new OSMTreeEntityFilter());
		singlePointFilter.put("trafficsignals", new OSMTrafficSignalEntityFilter());
		
		multiPointFilter.put("roads", new OSMRoadsEntityFilter());
		multiPointFilter.put("buildings", new OSMBuildingsEntityFilter());
	}
	
	public OSMFileReader(final String filename, final String type, final OSMStructureCallback structureCallback) {
		super();
		this.filename = filename;
		this.type = type;
		this.structureCallback = structureCallback;
	}

	protected class OSMSinglePointSink implements Sink {

		/**
		 * The entity filter
		 */
		private final OSMSinglePointEntityFilter entityFilter;

		protected OSMSinglePointSink(final OSMSinglePointEntityFilter entityFilter) {
			this.entityFilter = entityFilter;
		}

		@Override
		public void release() {
		}

		@Override
		public void complete() {
		}

		@Override
		public void initialize(final Map<String, Object> arg0) {
		}

		@Override
		public void process(final EntityContainer entityContainer) {
			
			if(entityContainer.getEntity() instanceof Node) {
				final Node node = (Node) entityContainer.getEntity();						
				
				if(entityFilter.forwardNode(node)) {
					final GeometricalStructure geometricalStructure = new GeometricalStructure(node.getId());
					geometricalStructure.addPoint(node.getLatitude(), node.getLongitude());
					structureCallback.processStructure(geometricalStructure);
				}
			}
		}
	}

	protected class OSMMultipointSink implements Sink {
		
		/**
		 * The node map
		 */
		private final Map<Long, Node> nodeMap = new HashMap<Long, Node>();
		
		/**
		 * The entity filter
		 */
		private final OSMMultiPointEntityFilter entityFilter;

		protected OSMMultipointSink(final OSMMultiPointEntityFilter entityFilter) {
			this.entityFilter = entityFilter;
		}

		@Override
		public void initialize(Map<String, Object> arg0) {
			
		}

		@Override
		public void complete() {
			
		}

		@Override
		public void release() {
			
		}

		@Override
		public void process(final EntityContainer entityContainer) {
			if(entityContainer.getEntity() instanceof Node) {
				final Node node = (Node) entityContainer.getEntity();						
				nodeMap.put(node.getId(), node);
			} else if(entityContainer.getEntity() instanceof Way) {
				final Way way = (Way) entityContainer.getEntity();
				final boolean forward = entityFilter.forwardNode(way.getTags());

				if(forward) {
					try {
						insertWay(way, nodeMap);
					} catch (ScalephantException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	/**
	 * Insert the given way into the scalephant
	 * @param way
	 * @param nodeMap 
	 * @throws ScalephantException 
	 */
	protected void insertWay(final Way way, final Map<Long, Node> nodeMap) throws ScalephantException {
		final GeometricalStructure geometricalStructure = new GeometricalStructure(way.getId());
		
		for(final WayNode wayNode : way.getWayNodes()) {
			
			if(! nodeMap.containsKey(wayNode.getNodeId())) {
				System.err.println("Unable to find node for way: " + wayNode.getNodeId());
				return;
			}
			
			final Node node = nodeMap.get(wayNode.getNodeId());
			geometricalStructure.addPoint(node.getLatitude(), node.getLongitude());
		}
		
		if(geometricalStructure.getNumberOfPoints() > 0) {
				structureCallback.processStructure(geometricalStructure);				
		}
		
	}
	
	
	/**
	 * Wait for the reader thread to complete
	 * 
	 * @param reader
	 */
	protected void waitForReaderThread(final OsmosisReader reader) {
		final Thread readerThread = new Thread(reader);
		readerThread.start();

		while (readerThread.isAlive()) {
		    try {
		        readerThread.join();
		        System.out.println("Done");
		    } catch (InterruptedException e) {
		        /* do nothing */
		    }
		}
	}
	
	/**
	 * Get the names of the available filter
	 * @return
	 */
	public static String getFilterNames() {
		final StringBuilder sb = new StringBuilder();
		
		final Set<String> names = getAllFilter();
		
		for(final String name : names) {
			sb.append(name);
			sb.append("|");
		}
		
		// Remove last '|'
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1);
		}
		
		return sb.toString();
	}

	/**
	 * Get all known filter
	 * @return
	 */
	public static Set<String> getAllFilter() {
		final Set<String> names = new HashSet<>();
		names.addAll(singlePointFilter.keySet());
		names.addAll(multiPointFilter.keySet());
		return Collections.unmodifiableSet(names);
	}


	/**
	 * Run the importer
	 * @throws ExecutionException 
	 */
	@Override
	public void run() {
		try {
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			
			if(singlePointFilter.containsKey(type)) {
				final OSMSinglePointEntityFilter entityFilter = singlePointFilter.get(type);
				reader.setSink(new OSMSinglePointSink(entityFilter));
			}
			
			if(multiPointFilter.containsKey(type)) {
				final OSMMultiPointEntityFilter entityFilter = multiPointFilter.get(type);			
				reader.setSink(new OSMMultipointSink(entityFilter));
			}
			
			waitForReaderThread(reader);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
}
