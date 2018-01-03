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
package org.bboxdb.tools;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.List;

import org.bboxdb.storage.StorageManagerException;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.storage.entity.TupleStoreName;
import org.bboxdb.storage.sstable.reader.SSTableFacade;
import org.bboxdb.storage.sstable.reader.SSTableKeyIndexReader;
import org.bboxdb.storage.sstable.reader.SSTableReader;
import org.bboxdb.storage.util.TupleHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SSTableExaminer implements Runnable {

	/**
	 * The base directory
	 */
	protected final String baseDirectory;
	
	/**
	 * The number of the table
	 */
	protected final int tableNumber;
	
	/**
	 * The name of the relation
	 */
	protected final TupleStoreName relationname;
	
	/**
	 * The key to examine
	 */
	protected final String examineKey;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableExaminer.class);

	
	
	public SSTableExaminer(final String baseDirectory, final TupleStoreName relationname, final int tableNumber, final String examineKey) {
		this.baseDirectory = baseDirectory;
		this.tableNumber = tableNumber;
		this.relationname = relationname;
		this.examineKey = examineKey;
	}

	@Override
	public void run() {
		try {			
			final SSTableFacade sstableFacade = new SSTableFacade(baseDirectory, relationname, tableNumber, 0);
			sstableFacade.init();
			
			if(! sstableFacade.acquire()) {
				throw new StorageManagerException("Unable to acquire sstable reader");
			}

			final SSTableReader ssTableReader = sstableFacade.getSsTableReader();
			final SSTableKeyIndexReader ssTableIndexReader = sstableFacade.getSsTableKeyIndexReader();

			fullTableScan(ssTableReader);
			internalScan(ssTableReader);
			seachViaIndex(ssTableReader, ssTableIndexReader);
			
			sstableFacade.release();
			sstableFacade.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	/**
	 * Search our tuple via index
	 * @param ssTableReader
	 * @param ssTableIndexReader
	 * @throws StorageManagerException
	 */
	protected void seachViaIndex(final SSTableReader ssTableReader,
			final SSTableKeyIndexReader ssTableIndexReader)
			throws StorageManagerException {
		
		System.out.println("Step3: Seach via index");
		final List<Integer> positions = ssTableIndexReader.getPositionsForTuple(examineKey);
		System.out.println("Got index pos: " + positions);
		
		// Tuple found
		for(final Integer position : positions) {
			System.out.println(ssTableReader.getTupleAtPosition(position));
		}
	}

	/**
	 * Perform a scan with the internal method of the sstable rader
	 * @param ssTableReader
	 * @throws StorageManagerException
	 */
	protected void internalScan(final SSTableReader ssTableReader)
			throws StorageManagerException {
		
		System.out.println("Step2: Scan for tuple with key: " + examineKey);
		final Tuple scanTuple = ssTableReader.scanForTuple(examineKey);
		System.out.println(scanTuple);
	}

	/**
	 * Perform a full table scan
	 * @param ssTableReader
	 * @throws IOException
	 */
	protected void fullTableScan(final SSTableReader ssTableReader)
			throws IOException {
		
		System.out.println("Step1: Looping over SSTable and searching for key: " + examineKey);
		while(true) {
			try {
				final Tuple tuple = TupleHelper.decodeTuple(ssTableReader.getMemory());
				if(tuple.getKey().equals(examineKey)) {
					System.out.println(tuple);
				}
				
			} catch (BufferUnderflowException e) {
				// Loop until the buffer is empty
				break;
			}
		}
	}
	
	/**
	 * Main * Main * Main 
	 * 
	 * Examine a given SSTable and the coresponding index. The tuple with the key=examineKey
	 * will be search with fulltable scans and index scans. The result of this operations
	 * is printed onto the console.
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		
		if(args.length != 4) {
			logger.error("Usage: SSTableExaminer <Base directory> <Tablename> <Tablenumber> <Key>");
			System.exit(-1);
		}
		
		final String baseDirectory = args[0];
		
		final TupleStoreName relationname = new TupleStoreName(args[1]);
		if(! relationname.isValid()) {
			logger.error("Relationname {} is invalid, exiting", args[0]);
			System.exit(-1);
		}
		
		try {
			final int tableNumber = Integer.parseInt(args[2]);
			
			final String examineKey = args[3];
			
			final SSTableExaminer dumper = new SSTableExaminer(baseDirectory, relationname, tableNumber, examineKey);
			dumper.run();
		} catch (NumberFormatException e) {
			logger.error("Unable to parse {} as tablenumber", args[2]);
			System.exit(-1);
		}
	}
}
