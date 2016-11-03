package de.fernunihagen.dna.scalephant.performance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.osmosis.OsmosisReader;
import de.fernunihagen.dna.scalephant.network.client.OperationFuture;
import de.fernunihagen.dna.scalephant.network.client.ScalephantException;
import de.fernunihagen.dna.scalephant.performance.osm.OSMMultiPointEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.OSMRoadsEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.OSMSinglePointEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.OSMTrafficSignalEntityFilter;
import de.fernunihagen.dna.scalephant.performance.osm.OSMTreeEntityFilter;
import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;

public class BenchmarkOSMInsertPerformance extends AbstractBenchmark {

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
					try {
						final BoundingBox boundingBox = new BoundingBox((float) node.getLatitude(), (float) node.getLatitude(), (float) node.getLongitude(), (float) node.getLongitude());
						final Tuple tuple = new Tuple(Long.toString(node.getId()), boundingBox, "abc".getBytes());
						final OperationFuture insertFuture = scalephantClient.insertTuple(table, tuple);
						
						// register pending future
						pendingFutures.add(insertFuture);
						checkForCompletedFutures();
						
						insertedTuples.incrementAndGet();
					} catch (ScalephantException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	protected class OSMMultipointSink implements Sink {
		
		/**
		 * The node map
		 */
		private final Map<Long, Node> nodeMap;
		
		/**
		 * The entity filter
		 */
		private final OSMMultiPointEntityFilter entityFilter;

		protected OSMMultipointSink(final Map<Long, Node> nodeMap, final OSMMultiPointEntityFilter entityFilter) {
			this.nodeMap = nodeMap;
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
	 * The amount of inserted tuples
	 */
	protected AtomicInteger insertedTuples = new AtomicInteger(0);
	
	/** 
	 * A 2 dimensional distribution group 
	 */
	protected final static String DISTRIBUTION_GROUP = "2_osmgroup";
	
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
	 * The name of the table to insert data into
	 */
	protected final String table;
	
	/**
	 * The replication factor
	 */
	protected final short replicationFactor;
	
	static {
		singlePointFilter.put("tree", new OSMTreeEntityFilter());
		singlePointFilter.put("trafficsignals", new OSMTrafficSignalEntityFilter());
		
		multiPointFilter.put("roads", new OSMRoadsEntityFilter());
	}

	public BenchmarkOSMInsertPerformance(final String filename, final String type, final short replicationFactor) {
		this.filename = filename;
		this.type = type;
		this.table = DISTRIBUTION_GROUP + "_" + type;
		this.replicationFactor = replicationFactor;
	}

	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException, ScalephantException {

		// Remove old data
		final OperationFuture deleteResult = scalephantClient.deleteDistributionGroup(DISTRIBUTION_GROUP);
		deleteResult.waitForAll();
		
		// Create a new distribution group
		final OperationFuture createResult = scalephantClient.createDistributionGroup(DISTRIBUTION_GROUP, replicationFactor);
		createResult.waitForAll();
		
		if(singlePointFilter.containsKey(type)) {
			runSinglePointBenchmark();
		} else if(multiPointFilter.containsKey(type)) {
			runMultiPointBenchmark();
		} else {
			System.err.println("Unknown filter: " + type);
		}
	}

	/**
	 * Run the benchmark for multi point objects
	 * @throws ExecutionException 
	 */
	protected void runMultiPointBenchmark() throws ExecutionException {
	
		final Map<Long, Node> nodeMap = new HashMap<Long, Node>();
		
		try {
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			final OSMMultiPointEntityFilter entityFilter = multiPointFilter.get(type);
			
			reader.setSink(new OSMMultipointSink(nodeMap, entityFilter));
			
			waitForReaderThread(reader);
		} catch (FileNotFoundException e) {
			throw new ExecutionException(e);
		}
	}

	/**
	 * Insert the given way into the scalephant
	 * @param way
	 * @param nodeMap 
	 * @throws ScalephantException 
	 */
	protected boolean insertWay(final Way way, final Map<Long, Node> nodeMap) throws ScalephantException {
		BoundingBox boundingBox = null;
		
		for(final WayNode wayNode : way.getWayNodes()) {
			
			if(! nodeMap.containsKey(wayNode.getNodeId())) {
				System.err.println("Unable to find node for way: " + wayNode.getNodeId());
				return false;
			}
			
			final Node node = nodeMap.get(wayNode.getNodeId());
			final BoundingBox nodeBoundingBox = new BoundingBox((float) node.getLatitude(), (float) node.getLatitude(), (float) node.getLongitude(), (float) node.getLongitude());
			
			if(boundingBox == null) {
				boundingBox = nodeBoundingBox;
			} else {
				boundingBox = BoundingBox.getBoundingBox(boundingBox, nodeBoundingBox);
			}
		}
		
		if(boundingBox != null) {
			final Tuple tuple = new Tuple(Long.toString(way.getId()), boundingBox, "abc".getBytes());
			final OperationFuture insertFuture = scalephantClient.insertTuple(table, tuple);
			
			// register pending future
			pendingFutures.add(insertFuture);
			checkForCompletedFutures();
			
			insertedTuples.incrementAndGet();
			
			return true;
		}
		
		return false;
	}
	
	/**
	 * Run the benchmark for single point objects
	 * @throws ExecutionException
	 */
	protected void runSinglePointBenchmark() throws ExecutionException {
		try {
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			final OSMSinglePointEntityFilter entityFilter = singlePointFilter.get(type);
			
			reader.setSink(new OSMSinglePointSink(entityFilter));
			
			waitForReaderThread(reader);
		} catch (FileNotFoundException e) {
			throw new ExecutionException(e);
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

	@Override
	protected DataTable getDataTable() {
		return new DataTable() {
			
			protected int lastInsertedTuples = 0;
			protected int diff = 0;
			
			@Override
			public String getValueForColum(short colum) {
				switch (colum) {
				
				// Total amount of inserted tuples
				case 0:
					final int tuples = insertedTuples.get();
					diff = tuples - lastInsertedTuples;
					lastInsertedTuples = tuples;
					return Integer.toString(tuples);
					
				// Diff amount of inserted tuples
				case 1:
					return Integer.toString(diff);

				default:
					return "-----";
				}
			}
			
			@Override
			public String getTableHeader() {
				return "#Time\tTuples\tTuples_per_sec";
			}
			
			@Override
			public short getColumns() {
				return 2;
			}
		};
	}	
	
	/* ====================================================
	 * Main
	 * ====================================================
	 */
	public static void main(final String[] args) throws InterruptedException, ExecutionException {
		
		// Check parameter
		if(args.length != 3) {
			System.err.println("Usage: programm <filename> <tree|trafficsignals|roads> <replication factor>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String type = args[1];
		final String replicationFactorString = args[2];
		short replicationFactor = -1;
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(-1);
		}
		
		// Check type
		if(! singlePointFilter.keySet().contains(type) && ! multiPointFilter.keySet().contains(type)) {
			System.err.println("Unknown type: " + type);
			System.exit(-1);
		}
		
		try {
			replicationFactor = Short.parseShort(replicationFactorString);
		} catch(NumberFormatException e) {
			System.err.println("Invalid replication factor: " + replicationFactorString);
			System.exit(-1);	
		}
		
		final BenchmarkOSMInsertPerformance benchmarkInsertPerformance = new BenchmarkOSMInsertPerformance(filename, type, replicationFactor);
		benchmarkInsertPerformance.run();
	}
	
}
