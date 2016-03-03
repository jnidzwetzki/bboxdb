package de.fernunihagen.dna.jkn.scalephant.storage.sstable.spatialindex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.entity.BoundingBox;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;

/**
 * Based on the Box Sort Algorithm by Piet Houthuys
 * @author kristofnidzwetzki
 *
 */
public class BoxSortIndex implements SpatialIndexer {

	/**
	 * The root element of the tree
	 */
	protected BoxNode rootElement;	
	
	public void constructFromTuples(final List<Tuple> tuples) {
		rootElement = partition(tuples, 0);
	}
	
	/**
	 * Create a binary search tree from the tuple list
	 * @param tuples
	 * @param dimension
	 * @return
	 */
	protected BoxNode partition(final List<Tuple> tuples, final int dimension) {
	
		sortTupleList(tuples, dimension);
		
		final int pivotPosition = tuples.size() / 2;
		final Tuple pivotTuple = tuples.get(pivotPosition);
		final int dimensions = pivotTuple.getBoundingBox().getDimension();
				
		final List<Tuple> smaller = new ArrayList<Tuple>();
		final List<Tuple> bigger = new ArrayList<Tuple>();
		
		splitTupleList(tuples, pivotTuple, smaller, bigger, dimension);
		
		final BoxNode subtreeRootnode = new BoxNode();
		subtreeRootnode.key = pivotTuple.getKey();
		subtreeRootnode.bbox = pivotTuple.getBoundingBox();
		
		subtreeRootnode.subtreeBoundingBox = pivotTuple.getBoundingBox();

	//	System.out.println("Splitted: " + tuples.size() + " into: " + smaller.size() + " and " + bigger.size());
		
		if(! smaller.isEmpty()) {
			subtreeRootnode.leftChild = partition(smaller, (dimension + 1) % (dimensions - 1));
			subtreeRootnode.subtreeBoundingBox = BoundingBox.getBoundingBox(subtreeRootnode.subtreeBoundingBox, subtreeRootnode.leftChild.subtreeBoundingBox);
		} else {
			subtreeRootnode.leftChild = null;
		}
		
		if(! bigger.isEmpty()) {
			subtreeRootnode.rightChild = partition(bigger, (dimension + 1) % (dimensions - 1));
			subtreeRootnode.subtreeBoundingBox = BoundingBox.getBoundingBox(subtreeRootnode.subtreeBoundingBox, subtreeRootnode.rightChild.subtreeBoundingBox);
		} else {
			subtreeRootnode.rightChild = null;
		}
				
		return subtreeRootnode;
	}

	/**
	 * Split the tuple list at the given piviotTuple
	 * @param tuples
	 * @param pivotTuple
	 * @param smaller
	 * @param bigger
	 * @param dimension
	 */
	protected void splitTupleList(final List<Tuple> tuples,
			final Tuple pivotTuple, final List<Tuple> smaller,
			final List<Tuple> bigger, final int dimension) {
		
		
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
		
	}

	/**
	 * Sort the list of tuples, relative to the dimension of the bounding box
	 * @param tuples
	 */
	protected void sortTupleList(final List<Tuple> tuples, final int dimension) {
		Collections.sort(tuples, new Comparator<Tuple>() {
			@Override
			public int compare(final Tuple tuple1, final Tuple tuple2) {
								
				final float coordinateLowTuple1 = tuple1.getBoundingBox().getCoordinateLow(dimension);
				final float coordinateLowTuple2 = tuple2.getBoundingBox().getCoordinateLow(dimension);
				
				if(coordinateLowTuple1 == coordinateLowTuple2) {
					return 0;
				} else if(coordinateLowTuple1 < coordinateLowTuple2) {
					return -1;
				} else {
					return 1;
				}

			}
		});
	}
	
	/**
	 * Query the tree, get all elements that are intersected by the 
	 * bounding box.
	 * 
	 * @param boundingBox
	 * @return
	 */
	public List<String> query(final BoundingBox boundingBox) {
		final List<String> resultList = new ArrayList<String>();

		query(boundingBox, rootElement, resultList);
		
		return resultList;
	}
	
	/**
	 * Query the tree recursively
	 * @param queryBoundingBox
	 * @param treeRoot
	 * @param resultList 
	 * @return
	 */
	protected void query(final BoundingBox queryBoundingBox, final BoxNode treeRoot, final List<String> resultList) {
		
		// Check parameter
		if(treeRoot == null || queryBoundingBox == null) {
			return;
		}
		
		// Element are inside the query box
		if(treeRoot.bbox.overlaps(queryBoundingBox)) {
			resultList.add(treeRoot.key);
		}
		
		// Recursive tree traversal (left child)
		if(treeRoot.leftChild != null) {
			if(treeRoot.leftChild.subtreeBoundingBox.overlaps(queryBoundingBox)) {
				query(queryBoundingBox, treeRoot.leftChild, resultList);
			}
		}
		
		// Recursive tree traversal (right child)
		if(treeRoot.rightChild != null) {
			if(treeRoot.rightChild.subtreeBoundingBox.overlaps(queryBoundingBox)) {
				query(queryBoundingBox, treeRoot.rightChild, resultList);
			}
		}
	}

	@Override
	public String toString() {
		return "BoxSortIndex [rootElement=" + rootElement + "]";
	}
	
}


/**
 * 
 * The class for the tree nodes
 *
 */
class BoxNode {
	protected BoxNode leftChild;
	protected BoxNode rightChild;
	protected String key;
	protected BoundingBox bbox;
	protected BoundingBox subtreeBoundingBox;
	
	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer();
		getRecursiveTree(sb, 0);
		return sb.toString();
	}
	
	public void getRecursiveTree(final StringBuffer sb, int deepth) {
	
		for(int i = 0; i < deepth; i++) {
			sb.append("\t");
		}
		
		sb.append("key: " + key + " bbox: " + bbox + " ");
		sb.append("subtreeBoundingBox: " + subtreeBoundingBox + "\n");
		
		if(leftChild != null) {
			leftChild.getRecursiveTree(sb, deepth + 1);
		}
		
		if(rightChild != null) {
			rightChild.getRecursiveTree(sb, deepth + 1);
		}
	}

}