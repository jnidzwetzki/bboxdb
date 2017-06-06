package org.bboxdb.tools.experiments.tuplestore;

import java.io.File;
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
	 * The JDBC h2 tuple store
	 */
	public final static String TUPLE_STORE_JDBC_H2 = "jdbc-h2";

	/**
	 * The JDBC derby tuple store
	 */
	public final static String TUPLE_STORE_JDBC_DERBY = "jdbc-derby";

	/**
	 * The SSTable tuple store
	 */
	public final static String TUPLE_STORE_SSTABLE = "sstable";
	
	/**
	 * All tuple stores
	 */
	public final static List<String> ALL_STORES = Collections.unmodifiableList(
			Arrays.asList(TUPLE_STORE_BDB, TUPLE_STORE_JDBC_H2, TUPLE_STORE_JDBC_DERBY,
					TUPLE_STORE_SSTABLE));
	
	/**
	 * Return the tuple store for the name
	 * @param tupleStoreName
	 * @return
	 * @throws IOException 
	 */
	public static TupleStore getTupleStore(final String tupleStoreName, final File dir) throws Exception {
		switch(tupleStoreName) {
		case TUPLE_STORE_BDB:
			return new BDBTupleStore(dir);
		case TUPLE_STORE_JDBC_H2:
			return new JDBCH2TupleStore(dir);
		case TUPLE_STORE_JDBC_DERBY:
			return new JDBCDerbyTupleStore(dir);
		case TUPLE_STORE_SSTABLE: 
			return new SSTableTupleStore(dir);
		default:
			throw new IllegalArgumentException("Unknown tuple store: " + tupleStoreName);
		}
	}

}
