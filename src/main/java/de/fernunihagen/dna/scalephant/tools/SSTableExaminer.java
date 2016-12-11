/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.tools;

import java.io.IOException;
import java.nio.BufferUnderflowException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.scalephant.ScalephantConfiguration;
import de.fernunihagen.dna.scalephant.ScalephantConfigurationManager;
import de.fernunihagen.dna.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableKeyIndexReader;
import de.fernunihagen.dna.scalephant.storage.sstable.reader.SSTableReader;

public class SSTableExaminer implements Runnable {

	/**
	 * The number of the table
	 */
	protected final int tableNumber;
	
	/**
	 * The name of the relation
	 */
	protected final SSTableName relationname;
	
	/**
	 * The key to examine
	 */
	protected final String examineKey;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableExaminer.class);
	
	
	public SSTableExaminer(final SSTableName relationname, final int tableNumber, final String examineKey) {
		this.tableNumber = tableNumber;
		this.relationname = relationname;
		this.examineKey = examineKey;
	}

	@Override
	public void run() {
		try {
			final ScalephantConfiguration storageConfiguration = ScalephantConfigurationManager.getConfiguration();
			
			final SSTableFacade sstableFacade = new SSTableFacade(storageConfiguration.getDataDirectory(), relationname, tableNumber);
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
		} catch (StorageManagerException e) {
			e.printStackTrace();
		} catch (IOException e) {
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
		int pos = ssTableIndexReader.getPositionForTuple(examineKey);
		System.out.println("Got index pos: " + pos);
		
		// Tuple found
		if(pos != -1) {
			System.out.println(ssTableReader.getTupleAtPosition(pos));
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
				final Tuple tuple = ssTableReader.decodeTuple();
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
		
		if(args.length != 3) {
			logger.error("Usage: SSTableExaminer <Tablename> <Tablenumber> <Key>");
			System.exit(-1);
		}
		
		final SSTableName relationname = new SSTableName(args[0]);
		if(! relationname.isValid()) {
			logger.error("Relationname {} is invalid, exiting", args[0]);
			System.exit(-1);
		}
		
		try {
			final int tableNumber = Integer.parseInt(args[1]);
			
			final String examineKey = args[2];
			
			final SSTableExaminer dumper = new SSTableExaminer(relationname, tableNumber, examineKey);
			dumper.run();
		} catch (NumberFormatException e) {
			logger.error("Unable to parse {} as tablenumber", args[1]);
			System.exit(-1);
		}
	}
}
