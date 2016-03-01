package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

public class BoxSearchIndex implements SpatialIndexer {

	/**
	 * The root element of the tree
	 */
	protected BoxNode rootElement;	
	
	public void constructFromTuples(final List<Tuple> tuples) {
		rootElement = partition(tuples, 1);
	}
	
	/**
	 * Annotate the tree with its min and max values
	 * 
	 * @param rootElement
	 */
	protected void annotateTree(final BoxNode rootElement) {
		
	}
	
	/**
	 * Create a binary search tree from the tuple list
	 * @param tuples
	 * @param dimension
	 * @return
	 */
	protected BoxNode partition(final List<Tuple> tuples, final int dimension) {
	
		sortTupleList(tuples);
		
		final int pivotPosition = tuples.size() / 2;
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
		
		if(! smaller.isEmpty()) {
			subtreeRootnode.leftChild = partition(smaller, dimension + 1 % dimensions);
		} else {
			subtreeRootnode.leftChild = null;
		}
		
		if(! bigger.isEmpty()) {
			subtreeRootnode.rightChild = partition(bigger, dimension + 1 % dimensions);
		} else {
			subtreeRootnode.rightChild = null;
		}
		
		return subtreeRootnode;
	}

	/**
	 * Sort the list of tuples, regarding the bounding box
	 * @param tuples
	 */
	protected void sortTupleList(final List<Tuple> tuples) {
		Collections.sort(tuples, new Comparator<Tuple>() {
			@Override
			public int compare(final Tuple tuple1, final Tuple tuple2) {
				return tuple1.getBoundingBox().compareTo(tuple2.getBoundingBox());
			}
		});
	}
	
}

class BoxNode {
	protected BoxNode leftChild;
	protected BoxNode rightChild;
	protected Tuple value;
	protected BoundingBox subtreeBoundingBox;
}