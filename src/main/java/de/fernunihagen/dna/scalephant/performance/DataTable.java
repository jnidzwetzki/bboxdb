package de.fernunihagen.dna.scalephant.performance;

public interface DataTable {

	/**
	 * Get the number of columns
	 * @return
	 */
	public short getColumns();
	
	/**
	 * Get the value for column
	 * @param colum
	 * @return
	 */
	public String getValueForColum(final short colum);
	
	/**
	 * Get the header of the table
	 * @return
	 */
	public String getTableHeader();
}
