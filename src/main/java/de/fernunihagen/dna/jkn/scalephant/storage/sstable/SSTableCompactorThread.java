package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.nio.channels.ClosedByInterruptException;
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

		final List<SSTableReader> reader = sstableManager.getSstableReader();
		
		while(sstableManager.isReady()) {
			
			try {	
				if(reader.size() > 2) {
					final SSTableReader reader1 = reader.get(0);
					final SSTableReader reader2 = reader.get(1);
					
					try {
						mergeSSTables(reader1, reader2);						
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
	protected void mergeSSTables(final SSTableReader reader1,
			final SSTableReader reader2) throws StorageManagerException {
		
		// Get the index reader for the file reader
		final SSTableIndexReader indexReader1 = sstableManager.getIndexReaderForTable(reader1);
		final SSTableIndexReader indexReader2 = sstableManager.getIndexReaderForTable(reader2);
		
		int tablenumber = sstableManager.increaseTableNumber();
		
		logger.info("Merging " + reader1.getTablebumber() + " and " + reader2.getTablebumber() + " into " + tablenumber);

		final SSTableWriter writer = new SSTableWriter(reader1.getName(), reader1.getDirectory(), tablenumber);
		final SSTableCompactor ssTableCompactor = new SSTableCompactor(Arrays.asList(indexReader1, indexReader2), writer);
		boolean compactSuccess = ssTableCompactor.executeCompactation();
		
		if(!compactSuccess) {
			logger.error("Error during compactation");
			return;
		}
		
		final SSTableReader newTableReader = new SSTableReader(reader1.getName(), reader1.getDirectory(), writer.getSstableFile());
		final SSTableIndexReader newTableIndexReader = new SSTableIndexReader(newTableReader);
		
		newTableReader.init();
		newTableIndexReader.init();
		
		// Register the new sstable reader
		sstableManager.getSstableReader().add(newTableReader);
		sstableManager.getIndexReader().put(newTableReader, newTableIndexReader);
		
		// Delete the files
		indexReader1.deleteOnClose();
		indexReader2.deleteOnClose();
		reader1.deleteOnClose();
		reader2.deleteOnClose();
		
		// Unregister the old tables
		sstableManager.getSstableReader().remove(reader1);
		sstableManager.getSstableReader().remove(reader2);
		sstableManager.getIndexReader().remove(reader1);
		sstableManager.getIndexReader().remove(reader2);
	}
}
