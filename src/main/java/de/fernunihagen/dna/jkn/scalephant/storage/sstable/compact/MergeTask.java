package de.fernunihagen.dna.jkn.scalephant.storage.sstable.compact;

import java.util.List;

import de.fernunihagen.dna.jkn.scalephant.storage.sstable.reader.SSTableFacade;

public class MergeTask {

	/**
	 * The tables that should be compacted by a minor compact
	 * @return
	 */
	protected List<SSTableFacade> minorCompactTables;
	
	/**
	 * The tables that should be compacted by a minor compact
	 * @return
	 */
	protected List<SSTableFacade> majorCompactTables;

	
	public List<SSTableFacade> getMinorCompactTables() {
		return minorCompactTables;
	}

	public void setMinorCompactTables(List<SSTableFacade> minorCompactTables) {
		this.minorCompactTables = minorCompactTables;
	}

	public List<SSTableFacade> getMajorCompactTables() {
		return majorCompactTables;
	}

	public void setMajorCompactTables(List<SSTableFacade> majorCompactTables) {
		this.majorCompactTables = majorCompactTables;
	}
	
}
