package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;

public class SSTableCompactorThread implements Runnable {
	
	/**
	 * The corresponding SSTable manager
	 */
	protected final SSTableManager sstableManager;
	
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

		final List<SSTableFacade> facades = sstableManager.getSstableFacades();
		
		while(sstableManager.isReady()) {
			
			try {	
				if(facades.size() > 2) {
					
					final SSTableFacade facade1 = facades.get(0);
					final SSTableFacade facade2 = facades.get(1);

					try {
						mergeSSTables(facade1, facade2);						
					} catch (Exception e) {
						
						if(Thread.currentThread().isInterrupted()) {
							logger.info("Compact thread for: " + sstableManager.getName() + " is done");
							return;
						}
						
						logger.error("Error while merging tables", e);
					} 
				}
			
				Thread.sleep(10 * 1000);
			} catch (InterruptedException e) {
				logger.info("Compact thread for: " + sstableManager.getName() + " is done");
				return;
			} 
		}
		
		logger.info("Compact thread for: " + sstableManager.getName() + " is done");
	}

	/**
	 * Merge two sstables into a new one
	 * @param reader1
	 * @param reader2
	 * @throws StorageManagerException
	 */
	protected void mergeSSTables(final SSTableFacade facade1,
			final SSTableFacade facade2) throws StorageManagerException {
		
		// Get the index reader for the file reader
		final SSTableKeyIndexReader indexReader1 = facade1.getSsTableKeyIndexReader();
		final SSTableKeyIndexReader indexReader2 = facade2.getSsTableKeyIndexReader();
		
		int tablenumber = sstableManager.increaseTableNumber();
		
		logger.info("Merging " + facade1.getTablebumber() + " and " + facade2.getTablebumber() + " into " + tablenumber);

		final SSTableWriter writer = new SSTableWriter(facade1.getDirectory(), facade1.getName(), tablenumber);
		final SSTableCompactor ssTableCompactor = new SSTableCompactor(Arrays.asList(indexReader1, indexReader2), writer);
		boolean compactSuccess = ssTableCompactor.executeCompactation();
		
		if(!compactSuccess) {
			logger.error("Error during compactation");
			return;
		}
		
		final SSTableFacade newFacade = new SSTableFacade(facade1.getDirectory(), facade1.getName(), tablenumber);
		newFacade.init();
		
		// Register the new sstable reader
		sstableManager.getSstableFacades().add(newFacade);

		// Delete the files
		facade1.deleteOnClose();
		facade2.deleteOnClose();
		
		// Unregister the old tables
		sstableManager.getSstableFacades().remove(facade1);
		sstableManager.getSstableFacades().remove(facade2);
	}
}
