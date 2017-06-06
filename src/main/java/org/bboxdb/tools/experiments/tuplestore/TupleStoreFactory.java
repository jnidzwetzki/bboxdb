package org.bboxdb.tools.experiments.tuplestore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TupleStoreFactory {
	
	/**
	 * The BDB tuple store
	 */
	public final static String TUPLE_STORE_BDB = "bdb";
	
	/**
	 * The JDBC tuple store
	 */
	public final static String TUPLE_STORE_JDBC = "jdbc";

	/**
	 * The SSTable tuple store
	 */
	public final static String TUPLE_STORE_SSTABLE = "sstable";
	
	/**
	 * All tuple stores
	 */
	public final static List<String> ALL_STORES = Collections.unmodifiableList(
			Arrays.asList(TUPLE_STORE_BDB, TUPLE_STORE_JDBC, TUPLE_STORE_SSTABLE));
	
	/**
	 * Return the tuple store for the name
	 * @param tupleStoreName
	 * @return
	 * @throws IOException 
	 */
	public static TupleStore getTupleStore(final String tupleStoreName) throws IOException {
		switch(tupleStoreName) {
		case TUPLE_STORE_BDB:
			return new BDBTupleStore();
		case TUPLE_STORE_JDBC:
			return null;
		case TUPLE_STORE_SSTABLE: 
			return null;
		default:
			throw new IllegalArgumentException("Unknown tuple store: " + tupleStoreName);
		}
	}

}
