package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class BoxSearchIndex implements SpatialIndexer {
	
	/**
	 * An instance to generate random numbers
	 */
	protected final Random random = new Random();
	
	/**
	 * The root element of the tree
	 */
	protected BoxNode rootElement;
	
	protected int getRandomPivotPosition(final int max) {
		return random.nextInt() % max;
	}
	
	public void constructFromTuples(final List<Tuple> tuples) {
		rootElement = partition(tuples, 1);
	}
	
	protected BoxNode partition(final List<Tuple> tuples, final int dimension) {
		final int pivotPosition = getRandomPivotPosition(tuples.size());
		final Tuple pivotTuple = tuples.get(pivotPosition);
		
		final List<Tuple> smaller = new ArrayList<Tuple>();
		final List<Tuple> bigger = new ArrayList<Tuple>();
		
		for(final Tuple tuple : tuples) {
			if(tuple == pivotTuple) {
				continue;
			}
			
			if(tuple.getBoundingBox().getCoordinateLow(dimension) 
					< pivotTuple.getBoundingBox().getCoordinateLow(dimension)) {
				smaller.add(tuple);
			} else {
				bigger.add(tuple);
			}
		}
		
		final BoxNode subtreeRootnode = new BoxNode();
		subtreeRootnode.value = pivotTuple;
		
		final int dimensions = pivotTuple.getBoundingBox().getDimension();
		subtreeRootnode.leftChild = partition(smaller, dimension + 1 % dimensions);
		subtreeRootnode.rightChild = partition(bigger, dimension + 1 % dimensions);
		return subtreeRootnode;
	}
	
}

class BoxNode {
	protected BoxNode leftChild;
	protected BoxNode rightChild;
	protected Tuple value;
}