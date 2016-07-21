package de.fernunihagen.dna.jkn.scalephant.performance;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import crosby.binary.osmosis.OsmosisReader;
import de.fernunihagen.dna.jkn.scalephant.network.client.OperationFuture;
import de.fernunihagen.dna.jkn.scalephant.performance.osm.OSMEntityFilter;
import de.fernunihagen.dna.jkn.scalephant.performance.osm.OSMTrafficSignalEntityFilter;
import de.fernunihagen.dna.jkn.scalephant.performance.osm.OSMTreeEntityFilter;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class BenchmarkOSMInsertPerformance extends AbstractBenchmark {

	/**
	 * The amount of inserted tuples
	 */
	protected AtomicInteger insertedTuples = new AtomicInteger(0);
	
	/** 
	 * A 3 dimensional table (member of distribution group 'mygroup3') with the name 'testdata'
	 */
	protected final static String DISTRIBUTION_GROUP = "2_osmgroup";
	
	/**
	 * The filter
	 */
	protected final static Map<String, OSMEntityFilter> filter = new HashMap<String, OSMEntityFilter>();
	
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
	
	static {
		filter.put("tree", new OSMTreeEntityFilter());
		filter.put("trafficsignals", new OSMTrafficSignalEntityFilter());
	}

	public BenchmarkOSMInsertPerformance(final String filename, final String type) {
		this.filename = filename;
		this.type = type;
		this.table = DISTRIBUTION_GROUP + "_" + type;
	}

	@Override
	public void runBenchmark() throws InterruptedException, ExecutionException {

		// Remove old data
		final OperationFuture deleteResult = scalephantClient.deleteDistributionGroup(DISTRIBUTION_GROUP);
		deleteResult.waitForAll();
		
		// Create a new distribution group
		final OperationFuture createResult = scalephantClient.createDistributionGroup(DISTRIBUTION_GROUP, (short) 3);
		createResult.waitForAll();
		
		try {
			final OsmosisReader reader = new OsmosisReader(new FileInputStream(filename));
			reader.setSink(new Sink() {
				
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
						final OSMEntityFilter entityFilter = filter.get(type);
						
						if(entityFilter.forwardNode(node)) {
							final BoundingBox boundingBox = new BoundingBox((float) node.getLatitude(), (float) node.getLatitude(), (float) node.getLongitude(), (float) node.getLongitude());
							final Tuple tuple = new Tuple(Long.toString(node.getId()), boundingBox, "abc".getBytes());
							scalephantClient.insertTuple(table, tuple);
							System.out.println("Insert: "  + tuple + " into " + table);
						}
					}
				}
			});
			
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
		} catch (FileNotFoundException e) {
			throw new ExecutionException(e);
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
		if(args.length != 2) {
			System.err.println("Usage: programm <filename> <type:tree|trafficsignals>");
			System.exit(-1);
		}
		
		final String filename = args[0];
		final String type = args[1];
		
		// Check file
		final File inputFile = new File(filename);
		if(! inputFile.isFile()) {
			System.err.println("Unable to open file: " + filename);
			System.exit(-1);
		}
		
		// Check type
		if(! filter.keySet().contains(type)) {
			System.err.println("Unknown type: " + type);
			System.exit(-1);
		}
		
		final BenchmarkOSMInsertPerformance benchmarkInsertPerformance = new BenchmarkOSMInsertPerformance(filename, type);
		benchmarkInsertPerformance.run();
	}

	
}
