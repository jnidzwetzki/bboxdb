package org.bboxdb.storage.sstable.spatialindex.rtree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexReader;

public abstract class AbstractRTreeReader implements SpatialIndexReader {

	/**
	 * The max size of a child node
	 */
	protected int maxNodeSize;
	
	/**
	 * Get the max node size for the index
	 * @return
	 */
	public int getMaxNodeSize() {
		return maxNodeSize;
	}
	
	/**
	 * Validate the magic bytes of a stream
	 * 
	 * @return a InputStream or null
	 * @throws StorageManagerException
	 * @throws IOException 
	 */
	protected void validateStream(final RandomAccessFile randomAccessFile) throws IOException, StorageManagerException {
		
		// Validate file - read the magic from the beginning
		final byte[] magicBytes = new byte[SSTableConst.MAGIC_BYTES_SPATIAL_RTREE_INDEX.length];
		randomAccessFile.readFully(magicBytes, 0, SSTableConst.MAGIC_BYTES_SPATIAL_RTREE_INDEX.length);

		if(! Arrays.equals(magicBytes, SSTableConst.MAGIC_BYTES_SPATIAL_RTREE_INDEX)) {
			throw new StorageManagerException("Spatial index file does not contain the magic bytes");
		}
	}
	
}
