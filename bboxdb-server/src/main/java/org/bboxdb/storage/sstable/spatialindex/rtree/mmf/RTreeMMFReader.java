/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
 *  
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License. 
 *    
 *******************************************************************************/
package org.bboxdb.storage.sstable.spatialindex.rtree.mmf;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.stream.Collectors;

import org.bboxdb.commons.io.DataEncoderHelper;
import org.bboxdb.commons.io.UnsafeMemoryHelper;
import org.bboxdb.commons.math.BoundingBox;
import org.bboxdb.misc.Const;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.rtree.AbstractRTreeReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RTreeMMFReader extends AbstractRTreeReader {

	/**
	 * The mapped memory
	 */
	private MappedByteBuffer memory;
	
	/**
	 * The file channel
	 */
	private FileChannel fileChannel;

	/**
	 * The position of the first node
	 */
	private int firstNodePos;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(RTreeMMFReader.class);

	@Override
	public void readFromFile(final RandomAccessFile randomAccessFile) 
			throws StorageManagerException, InterruptedException {
		
		try {
			validateStream(randomAccessFile);
			maxNodeSize = DataEncoderHelper.readIntFromDataInput(randomAccessFile);
			
			firstNodePos = (int) randomAccessFile.getFilePointer();
			
			fileChannel = randomAccessFile.getChannel();
			final long size = fileChannel.size();
			memory = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
			memory.order(Const.APPLICATION_BYTE_ORDER);
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}

	@Override
	public void close() {
		
		if(memory != null) {
			UnsafeMemoryHelper.unmapMemory(memory);
			memory = null;
		}
		
		if(fileChannel != null) {
			try {
				fileChannel.close();
			} catch (IOException e) {
				logger.error("Got IO exception while closing file channel", e);
			}
			fileChannel = null;
		}
	}

	@Override
	public synchronized List<SpatialIndexEntry> getEntriesForRegion(final BoundingBox boundingBox) 
			throws StorageManagerException {
		
		final List<SpatialIndexEntry> resultList = new ArrayList<>();
		final Queue<Integer> readTasks = new LinkedTransferQueue<>();
		readTasks.add(firstNodePos);
		
		try {
			
			while(! readTasks.isEmpty()) {
			
				final int position = readTasks.remove();
				memory.position(position);
				final DirectoryNode directoryNode = new DirectoryNode();
				directoryNode.initFromByteBuffer(memory, maxNodeSize);
				
				if(directoryNode.getBoundingBox().overlaps(boundingBox)) {
					readTasks.addAll(directoryNode.getChildNodes());
					
					final List<SpatialIndexEntry> foundEntries = 
						directoryNode.getIndexEntries()
						.stream()
						.filter(e -> e.getBoundingBox().overlaps(boundingBox))
						.collect(Collectors.toList());
					
					resultList.addAll(foundEntries);
				}
			}
			
			return resultList;
		} catch (IOException e) {
			throw new StorageManagerException(e);
		}
	}

}
