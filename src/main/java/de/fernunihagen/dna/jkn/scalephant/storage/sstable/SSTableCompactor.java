package de.fernunihagen.dna.jkn.scalephant.storage.sstable;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.storage.StorageManagerException;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class SSTableCompactor implements Runnable {

	/**
	 * The first sstables to compact
	 */
	protected final SSTableIndexReader sstableIndexReader1;
	
	/**
	 * The second sstables to compact
	 */
	protected final SSTableIndexReader sstableIndexReader2;

	/**
	 * Our output sstable writer
	 */
	protected final SSTableWriter sstableWriter;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(SSTableCompactor.class);

	public SSTableCompactor(final SSTableIndexReader sstableIndexReader1, 
			final SSTableIndexReader sstableIndexReader2, 
			final SSTableWriter sstableWriter) {
		
		super();
		this.sstableIndexReader1 = sstableIndexReader1;
		this.sstableIndexReader2 = sstableIndexReader2;
		this.sstableWriter = sstableWriter;
	}
	
	/** 
	 * Execute the compactation of the input sstables
	 * 
	 * @return success or failure
	 */
	public boolean executeCompactation() {

		// Open iterators for input sstables
		final Iterator<Tuple> iterator1 = sstableIndexReader1.iterator();
		final Iterator<Tuple> iterator2 = sstableIndexReader2.iterator();

		Tuple tuple1 = null;
		Tuple tuple2 = null;
		
		try {
			sstableWriter.open();
			logger.info("Execute a new compactation into file " + sstableWriter.getSstableFile());

			while(iterator1.hasNext() || iterator2.hasNext() || tuple1 != null || tuple2 != null) {
				if(iterator1.hasNext() && tuple1 == null) {
					tuple1 = iterator1.next();
				}
				
				if(iterator2.hasNext() && tuple2 == null) {
					tuple2 = iterator2.next();
				}
				
				// Stream1 is exhausted
				if(tuple1 == null) {
					sstableWriter.addNextTuple(tuple2);
					while(iterator2.hasNext()) {
						tuple2 = iterator2.next();
						sstableWriter.addNextTuple(tuple2);
					}
					
					break;
				}
				
				// Stream2 is exhausted
				if(tuple2 == null) {
					sstableWriter.addNextTuple(tuple1);
					while(iterator1.hasNext()) {
						tuple1 = iterator1.next();
						sstableWriter.addNextTuple(tuple1);
					}
					
					break;
				}
				
				int result = tuple1.getKey().compareTo(tuple2.getKey());
				
				if(result == 0) {
					if(tuple1.getTimestamp() > tuple2.getTimestamp()) {
						sstableWriter.addNextTuple(tuple1);
						tuple1 = null;
						tuple2 = null;
					} else {
						sstableWriter.addNextTuple(tuple2);
						tuple1 = null;
						tuple2 = null;
					}
				} else if(result < 0) {
					sstableWriter.addNextTuple(tuple1);
					tuple1 = null;
				} else {
					sstableWriter.addNextTuple(tuple2);
					tuple2 = null;
				}
			}
			
			sstableWriter.close();
		} catch (StorageManagerException e) {
			logger.error("Exception while compatation", e);
			return false;
		}
		
		return true;
	}

	@Override
	public void run() {
		executeCompactation();
	}
}
