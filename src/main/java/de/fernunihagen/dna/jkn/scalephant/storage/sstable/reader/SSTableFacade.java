package de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.ScalephantService;
import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SStableMetaData;
import de.fernunihagen.dna.jkn.scalephant.storage.sstable.SSTableHelper;

public class SSTableFacade implements ScalephantService {
	 
	/**
	 * The name of the table
	 */
	protected final SSTableName name;
	
	/**
	 * The Directoy for the SSTables
	 */
	protected final String directory;
	
	/**
	 * The SStable reader
	 */
	protected final SSTableReader ssTableReader;
	
	/**
	 * The SSTable Key index reader
	 */
	protected final SSTableKeyIndexReader ssTableKeyIndexReader;
	
	/**
	 * The metadata of the sstable
	 */
	protected final SStableMetaData ssTableMetadata;
	
	/**
	 * The number of the table
	 */
	protected final int tablenumber;
	
	/**
	 * The usage counter
	 */
	protected final AtomicInteger usage;
	
	/**
	 * Delete file on close
	 */
	protected volatile boolean deleteOnClose; 
	
	/**
	 * The Logger
	 */
	protected static final Logger logger = LoggerFactory.getLogger(SSTableFacade.class);
	

	public SSTableFacade(final String directory, final SSTableName tablename, final int tablenumber) throws StorageManagerException {
		super();
		this.name = tablename;
		this.directory = directory;
		this.tablenumber = tablenumber;
		
		ssTableReader = new SSTableReader(directory, tablename, tablenumber);
		ssTableKeyIndexReader = new SSTableKeyIndexReader(ssTableReader);
		
		final File metadataFile = getMetadataFile(directory, tablename, tablenumber);
		ssTableMetadata = SStableMetaData.importFromYamlFile(metadataFile);
		
		this.usage = new AtomicInteger(0);
		deleteOnClose = false;
	}

	protected File getMetadataFile(final String directory,
			final SSTableName tablename, final int tablenumber) {
		final String metadatafile = SSTableHelper.getSSTableMetadataFilename(directory, tablename.getFullname(), tablenumber);
		final File metadataFile = new File(metadatafile);
		return metadataFile;
	}

	@Override
	public void init() {
		
		if(ssTableReader == null || ssTableKeyIndexReader == null) {
			logger.warn("init called but sstable reader or index reader is null");
			return;
		}
		
		ssTableReader.init();
		ssTableKeyIndexReader.init();
	}

	@Override
	public void shutdown() {
		
		if(ssTableReader == null || ssTableKeyIndexReader == null) {
			logger.warn("shutdown called but sstable reader or index reader is null");
			return;
		}
		
		ssTableKeyIndexReader.shutdown();
		ssTableReader.shutdown();
	}

	@Override
	public String getServicename() {
		return "SSTable facade for: " + name + " " + tablenumber;
	}

	@Override
	public String toString() {
		return "SSTableFacade [name=" + name + ", directory=" + directory
				+ ", tablebumber=" + tablenumber + "]";
	}

	public SSTableReader getSsTableReader() {
		return ssTableReader;
	}

	public SSTableKeyIndexReader getSsTableKeyIndexReader() {
		return ssTableKeyIndexReader;
	}
	

	/**
	 * Delete the underlying file as soon as usage == 0
	 */
	public void deleteOnClose() {
		deleteOnClose = true;
		testFileDelete();
	}
	
	/** 
	 * Increment the usage counter
	 * @return
	 */
	public boolean acquire() {
		
		// We are closing this instance
		if(deleteOnClose == true) {
			return false;
		}
		
		usage.incrementAndGet();
		return true;
	}
	
	/**
	 * Decrement the usage counter
	 */
	public void release() {
		usage.decrementAndGet();
		
		testFileDelete();
	}

	/**
	 * Delete the file if possible
	 */
	protected void testFileDelete() {
		if(deleteOnClose && usage.get() == 0) {
			logger.info("Delete service facade for: " + name + " / " + tablenumber);
			
			shutdown();
			
			if(ssTableKeyIndexReader != null) {
				ssTableKeyIndexReader.delete();
			}
			
			if(ssTableReader != null) {
				ssTableReader.delete();
			}
			
			// Delete metadata
			final File metadataFile = getMetadataFile(directory, name, tablenumber);
			metadataFile.delete();
		}
	}

	public SSTableName getName() {
		return name;
	}

	public String getDirectory() {
		return directory;
	}

	public int getTablebumber() {
		return tablenumber;
	}

	public AtomicInteger getUsage() {
		return usage;
	}

	public SStableMetaData getSsTableMetadata() {
		return ssTableMetadata;
	}
	
}
