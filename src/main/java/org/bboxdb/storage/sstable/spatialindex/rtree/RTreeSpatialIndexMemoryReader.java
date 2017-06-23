package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.BoundingBox;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexReader;
import org.bboxdb.util.io.DataEncoderHelper;

import com.google.common.io.ByteStreams;

public class RTreeSpatialIndexMemoryReader implements SpatialIndexReader {

	/**
	 * The root node of the tree
	 */
	protected RTreeDirectoryNode rootNode;
	
	/**
	 * The max size of a child node
	 */
	protected int maxNodeSize;
	
	
	public RTreeSpatialIndexMemoryReader() {
		
	}
	
	public RTreeSpatialIndexMemoryReader(final RTreeSpatialIndexBuilder indexBuilder) {
		this.rootNode = indexBuilder.rootNode;
		this.maxNodeSize = indexBuilder.maxNodeSize;
	}

	@Override
	public void readFromStream(final InputStream inputStream) throws StorageManagerException, InterruptedException {
		
		assert (rootNode == null);
		
		try {
			// Validate the magic bytes
			validateStream(inputStream);
			maxNodeSize = DataEncoderHelper.readIntFromStream(inputStream);
			rootNode = RTreeDirectoryNode.readFromStream(inputStream, maxNodeSize);
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}

	@Override
	public List<? extends SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox) {
		return rootNode.getEntriesForRegion(boundingBox);
	}
	
	/**
	 * Validate the magic bytes of a stream
	 * 
	 * @return a InputStream or null
	 * @throws StorageManagerException
	 * @throws IOException 
	 */
	protected static void validateStream(final InputStream inputStream) throws IOException, StorageManagerException {
		
		// Validate file - read the magic from the beginning
		final byte[] magicBytes = new byte[SSTableConst.MAGIC_BYTES_SPATIAL_RTREE_INDEX.length];
		ByteStreams.readFully(inputStream, magicBytes, 0, SSTableConst.MAGIC_BYTES_SPATIAL_RTREE_INDEX.length);

		if(! Arrays.equals(magicBytes, SSTableConst.MAGIC_BYTES_SPATIAL_RTREE_INDEX)) {
			throw new StorageManagerException("Spatial index file does not contain the magic bytes");
		}
	}
	
	/**
	 * Get the max node size for the index
	 * @return
	 */
	public int getMaxNodeSize() {
		return maxNodeSize;
	}
}
