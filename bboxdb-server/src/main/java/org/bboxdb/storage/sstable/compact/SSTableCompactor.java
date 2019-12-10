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
package org.bboxdb.storage.sstable.compact;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.bboxdb.commons.DuplicateResolver;
import org.bboxdb.commons.SortedIteratorMerger;
import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.DeletedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.SSTableConst;
import org.bboxdb.storage.sstable.SSTableCreator;
import org.bboxdb.storage.sstable.SSTableWriter;
import org.bboxdb.storage.sstable.duplicateresolver.TupleDuplicateResolverFactory;
import org.bboxdb.storage.sstable.reader.SSTableKeyIndexReader;
import org.bboxdb.storage.tuplestore.manager.TupleStoreManager;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableCompactor {

	/**
	 * Major or minor compaction? In a major compaction, the deleted tuple
	 * marker can be removed.
	 */
	private boolean majorCompaction = false;

	/**
	 * The amount of read tuples
	 */
	private int readTuples;

	/**
	 * The amount of written tuples
	 */
	private int writtenTuples;

	/**
	 * The list of sstables to compact
	 */
	private final List<SSTableKeyIndexReader> sstableIndexReader;

	/**
	 * The current SStable writer
	 */
	private SSTableWriter sstableWriter;

	/**
	 * The SStable manager
	 */
	private final TupleStoreManager tupleStoreManager;

	/**
	 * The resulting writer
	 */
	private final List<SSTableWriter> resultList = new ArrayList<>();

	/**
	 * Was the compactification successfully
	 */
	private boolean successfully = true;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCompactor.class);

	public SSTableCompactor(final TupleStoreManager sstableManager,
			final List<SSTableKeyIndexReader> sstableIndexReader) {

		this.tupleStoreManager = sstableManager;
		this.sstableIndexReader = sstableIndexReader;
		this.readTuples = 0;
		this.writtenTuples = 0;
	}

	/**
	 * Calculate max the number of entries in the output
	 * @param tables
	 * @return
	 */
	private long calculateNumberOfEntries(final List<SSTableKeyIndexReader> indexReader) {
		return indexReader
			.stream()
			.mapToInt(r -> r.getNumberOfEntries())
			.sum();
	}

	/**
	 * Execute the compactation of the input sstables
	 *
	 * @return success or failure
	 */
	public void executeCompactation() throws StorageManagerException {

		try {
			// The iterators
			final List<Iterator<Tuple>> iterators = sstableIndexReader
					.stream()
					.map(r -> r.iterator())
					.collect(Collectors.toList());

			final DuplicateResolver<Tuple> newestKeyResolver = TupleDuplicateResolverFactory.build(
					tupleStoreManager.getTupleStoreConfiguration());

			final SortedIteratorMerger<Tuple> sortedIteratorMerger = new SortedIteratorMerger<>(
					iterators,
					TupleHelper.TUPLE_KEY_COMPARATOR,
					newestKeyResolver);

			for(final Tuple tuple : sortedIteratorMerger) {
				checkForTermination(tuple);
				addTupleToWriter(tuple);
			}

			readTuples = sortedIteratorMerger.getReadElements();
		} catch (StorageManagerException e) {
			handleErrorDuringCompact(e);
		} finally {
			closeSSTableWriter();
		}
	}


	/**
	 * Check for the thread termination
	 * @param tuple
	 * @throws StorageManagerException
	 */
	private void checkForTermination(final Tuple tuple) throws StorageManagerException {

		if(Thread.currentThread().isInterrupted()) {
			throw new StorageManagerException("The curent thread is interrupted, stop compact");
		}

		if(tuple == null) {
			throw new StorageManagerException("Got a null tuple, stop compact");
		}
	}

	/**
	 *  Deleted tuples can be removed in a major compactification
	 *  only when no duplicate keys are allowed. Otherwise this is needed to
	 *  invalidate tuples in the tuple history
	 * @return
	 */
	private boolean skipDeletedTuplesToOutput() {
		if(! isMajorCompaction()) {
			return false;
		}

		if(tupleStoreManager.getTupleStoreConfiguration().isAllowDuplicates()) {
			return false;
		}

		return true;
	}

	/**
	 * Add the given tuple to the output file
	 * @param tuple
	 * @throws StorageManagerException
	 */
	private void addTupleToWriter(final Tuple tuple) throws StorageManagerException {

		if(tuple instanceof DeletedTuple && skipDeletedTuplesToOutput()) {
			return;
		}

		openNewWriterIfNeeded(tuple);
		sstableWriter.addTuple(tuple);
		writtenTuples++;
	}

	/**
	 * Handle the error during compact
	 * @param e
	 * @throws StorageManagerException
	 */
	private void handleErrorDuringCompact(final StorageManagerException e)
			throws StorageManagerException {

		successfully = false;

		closeSSTableWriter();

		// Delete partial written results
		logger.debug("Deleting partial written results");
		resultList.forEach(s -> s.deleteFromDisk());
		resultList.clear();

		final boolean oneNotReady = sstableIndexReader
				.stream()
				.anyMatch(r -> ! r.isReady());

		if(oneNotReady) {
			logger.debug("Suppressing exception on shutdown reader", e);
		} else {
			throw e;
		}
	}

	/**
	 * Close the open sstable writer
	 */
	private void closeSSTableWriter() {
		// Close open writer
		if(sstableWriter == null) {
			return;
		}

		try {
			sstableWriter.close();
			sstableWriter = null;
		} catch (StorageManagerException e) {
			logger.error("Got an exception while closing writer in error handler", e);
		}
	}

	/**
	 * Create a new table if the size of the open table hits the threshold
	 * @param resultList
	 * @param tuple
	 * @throws StorageManagerException
	 */
	private void openNewWriterIfNeeded(final Tuple tuple)
			throws StorageManagerException {

		if(sstableWriter == null) {
			sstableWriter = openNewSSTableWriter();
			return;
		}

		// Check max table size limit
		if(sstableWriter.getWrittenBytes() + tuple.getSize() > SSTableConst.MAX_SSTABLE_SIZE) {
			sstableWriter.close();
			sstableWriter = openNewSSTableWriter();
		}
	}

	/**
	 * Open a new SSTable writer
	 * @param resultList
	 * @return
	 * @throws StorageManagerException
	 */
	private SSTableWriter openNewSSTableWriter()
			throws StorageManagerException {

		final long estimatedMaxNumberOfEntries = calculateNumberOfEntries(sstableIndexReader);

		final String directory = sstableIndexReader.get(0).getDirectory();
		final int tablenumber = tupleStoreManager.increaseTableNumber();
		final TupleStoreName tupleStoreName = tupleStoreManager.getTupleStoreName();
		final SSTableCreator creatorType = getCreatorType();
		
		final SSTableWriter sstableWriter = new SSTableWriter(directory, tupleStoreName,
				tablenumber, estimatedMaxNumberOfEntries, creatorType);

		sstableWriter.open();
		resultList.add(sstableWriter);
		logger.info("Output file for compact: {}", sstableWriter.getSstableFile());
		return sstableWriter;
	}
	
	/**
	 * Get the SSTable creator type
	 * @return 
	 */
	private SSTableCreator getCreatorType() {
		if(isMajorCompaction()) {
			return SSTableCreator.MAJOR_COMPACT;
		}
		
		return SSTableCreator.MINOR_COMPACT;
	}


	/**
	 * Is this a major compaction?
	 * @return
	 */
	public boolean isMajorCompaction() {
		return majorCompaction;
	}

	/**
	 * Set major compaction flag
	 * @param majorCompaction
	 */
	public void setMajorCompaction(boolean majorCompaction) {
		this.majorCompaction = majorCompaction;
	}

	/**
	 * Get the amount of read tuples
	 * @return
	 */
	public int getReadTuples() {
		return readTuples;
	}

	/**
	 * Get the amount of written tuples
	 * @return
	 */
	public int getWrittenTuples() {
		return writtenTuples;
	}

	/**
	 * Get the writer result list
	 * @return
	 */
	public List<SSTableWriter> getResultList() {
		return resultList;
	}

	/**
	 * Was the compact successfully finished
	 * @return
	 */
	public boolean isSuccessfullyFinished() {
		return successfully;
	}
}
