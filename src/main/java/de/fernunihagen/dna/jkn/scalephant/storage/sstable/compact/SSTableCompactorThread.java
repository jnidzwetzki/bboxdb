package de.fernunihagen.dna.jkn.scalephant.storage.sstable.compact;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableManager;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableWriter;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableKeyIndexReader;

public class SSTableCompactorThread implements Runnable {
	
	/**
	 * The corresponding SSTable manager
	 */
	protected final SSTableManager sstableManager;
	
	/**
	 * The merge strategy
	 */
	protected final MergeStrategy mergeStragegy = new SimpleMergeStrategy();
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCompactorThread.class);

	public SSTableCompactorThread(final SSTableManager ssTableManager) {
		this.sstableManager = ssTableManager;
	}

	/**
	 * Compact our SSTables
	 * 
	 */
	@Override
	public void run() {
		logger.info("Starting new compact thread for: " + sstableManager.getName());

		while(sstableManager.isReady()) {

			try {	
				Thread.sleep(mergeStragegy.getCompactorDelay());
				logger.debug("Executing compact thread for: " + sstableManager.getName());

				// Create a copy to ensure, that the list of facades don't change
				// during the compact run.
				final List<SSTableFacade> facades = new ArrayList<SSTableFacade>(sstableManager.getSstableFacades());
				final MergeTask mergeTask = mergeStragegy.getMergeTasks(facades);
					
				try {
					mergeSSTables(mergeTask.getMinorCompactTables(), false);
					mergeSSTables(mergeTask.getMajorCompactTables(), true);				
				} catch (Exception e) {
					handleExceptionDuringMerge(e);
				} 
			} catch (InterruptedException e) {
				logger.info("Compact thread for: " + sstableManager.getName() + " is done");
				return;
			} 
		}
		
		logger.info("Compact thread for: " + sstableManager.getName() + " is done");
	}

	/**
	 * Handle exceptions during merge, only print the exception
	 * when the compact thread is not interrupted
	 * 
	 * @param e
	 */
	protected void handleExceptionDuringMerge(Exception e) {
		if(Thread.currentThread().isInterrupted()) {
			logger.info("Compact thread for: " + sstableManager.getName() + " is done");
		} else {
			logger.error("Error while merging tables", e);
		}
	}

	/**
	 * Merge two sstables into a new one
	 * @param reader1
	 * @param reader2
	 * @throws StorageManagerException
	 */
	protected void mergeSSTables(final List<SSTableFacade> tables, final boolean majorCompaction) throws StorageManagerException {
	
		if(tables == null || tables.isEmpty()) {
			return;
		}
		
		final String diretory = tables.get(0).getDirectory();
		final String name = tables.get(0).getName();
		
		final int tablenumber = sstableManager.increaseTableNumber();
		final SSTableWriter writer = new SSTableWriter(diretory, name, tablenumber);
		
		final List<SSTableKeyIndexReader> reader = new ArrayList<SSTableKeyIndexReader>();
		for(final SSTableFacade facade : tables) {
			reader.add(facade.getSsTableKeyIndexReader());
		}
		
		// Log the compact call
		if(logger.isInfoEnabled()) {
			writeMergeLog(tables, tablenumber, majorCompaction);
		}
		
		// Run the compact process
		final SSTableCompactor ssTableCompactor = new SSTableCompactor(reader, writer);
		ssTableCompactor.setMajorCompaction(majorCompaction);
		boolean compactSuccess = ssTableCompactor.executeCompactation();
		
		if(!compactSuccess) {
			logger.error("Error during compactation");
			return;
		}
		
		// Create a new facade and remove the old ones
		final SSTableFacade newFacade = new SSTableFacade(diretory, name, tablenumber);
		newFacade.init();
		
		// Register the new sstable reader
		sstableManager.getSstableFacades().add(newFacade);

		// Unregister and delete the files
		for(final SSTableFacade facade : tables) {
			facade.deleteOnClose();
			sstableManager.getSstableFacades().remove(facade);
		}
	}

	/***
	 * Write info about the merge run into log
	 * @param tables
	 * @param tablenumber
	 */
	protected void writeMergeLog(final List<SSTableFacade> tables, final int tablenumber, 
			final boolean majorCompaction) {
		
		final StringBuilder sb = new StringBuilder("Merging (major: ");
		sb.append(majorCompaction);
		sb.append(" [");
		
		for(final SSTableFacade facade : tables) {
			sb.append(facade.getTablebumber());
			sb.append(", ");
		}
		
		sb.append(" into ");
		sb.append(tablenumber);
		logger.info(sb.toString());
	}
}
