package de.fernunihagen.dna.jkn.scalephant.network;

import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class TupleAndTable {

	/**
	 * The tuple
	 */
	protected Tuple tuple;
	
	/**
	 * The table
	 */
	protected String table;
	
	public TupleAndTable(final Tuple tuple, final String table) {
		super();
		this.tuple = tuple;
		this.table = table;
	}

	public Tuple getTuple() {
		return tuple;
	}

	public void setTuple(Tuple tuple) {
		this.tuple = tuple;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}
	
}
