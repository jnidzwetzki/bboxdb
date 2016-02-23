package de.fernunihagen.dna.jkn.scalephant.storage.entity;


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

	public void setTuple(final Tuple tuple) {
		this.tuple = tuple;
	}

	public String getTable() {
		return table;
	}

	public void setTable(final String table) {
		this.table = table;
	}
	
}
