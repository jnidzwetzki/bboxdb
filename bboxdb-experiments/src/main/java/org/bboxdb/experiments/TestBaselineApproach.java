/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 *******************************************************************************/
package org.bboxdb.experiments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.TupleBuilder;
import org.bboxdb.tools.converter.tuple.TupleBuilderFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.google.common.base.Stopwatch;

public class TestBaselineApproach implements AutoCloseable {

	/**
	 * The cassandra cluster object
	 */
	private Cluster cluster;

	/**
	 * The cassandra session object
	 */
	private Session session;

	public TestBaselineApproach(final Cluster cluster) {
		this.cluster = cluster;
		this.session = cluster.connect();
	}

	@Override
	public void close() throws Exception {
		session.close();
		cluster.close();
	}

	/**
	 * Import data into cassandra
	 * @param args
	 */
	public void executeImport(final String args[]) {
		if(args.length != 4) {
			System.err.println("Usage: import <file> <desttable>");
			System.exit(-1);
		}

		executeImport(args[1], args[2]);
	}

	/**
	 * Import data into cassandra
	 * @param sourceFile
	 * @param format
	 * @param destTable
	 */
	public void executeImport(final String sourceFile, final String destTable) {
		session.execute("CREATE TABLE " + destTable + "(id long, data text, PRIMARY KEY(id)");

		final PreparedStatement prepared = session.prepare("INSERT INTO " + destTable
				+ " (id, text) values (?, ?)");

		long lineNumber = 0;
		String line = null;

		try(
				final BufferedReader br = new BufferedReader(new FileReader(new File(sourceFile)));
		) {

			while((line = br.readLine()) != null) {
				session.execute(prepared.bind(lineNumber, line));
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Execute a range query
	 * @param args
	 */
	public void executeRangeQuery(final String args[]) {
		if(args.length != 4) {
			System.err.println("Usage: rangequery <desttable> <format> <range>");
			System.exit(-1);
		}

		final Hyperrectangle hyperrectangle = Hyperrectangle.fromString(args[3]);

		executeRangeQuery(args[1], args[2], hyperrectangle);
	}

	/**
	 * Execute a range query
	 * @param destTable
	 * @param range
	 */
	public void executeRangeQuery(final String destTable, final String format, final Hyperrectangle range) {
		System.out.println("# Execute range query in range " + range);

		final Stopwatch stopwatch = Stopwatch.createStarted();
		final TupleBuilder tupleBuilder = TupleBuilderFactory.getBuilderForFormat(format);

		long readRecords = 0;
		long resultRecords = 0;

		final SimpleStatement statement = new SimpleStatement("SELECT * FROM " + destTable);
		statement.setFetchSize(2000); // Use 2000 tuples per page

		final ResultSet rs = session.execute(statement);

		for (final Row row : rs) {

			// Request next page
		    if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched()) {
		        rs.fetchMoreResults(); // this is asynchronous
		    }

		    readRecords++;

		    final long id = row.getLong(0);
		    final String text = row.getString(1);

		    final Tuple tuple = tupleBuilder.buildTuple(text, Long.toString(id));

		    if(tuple.getBoundingBox().intersects(range)) {
		    	resultRecords++;
		    }
		}

		System.out.println("# Read records " + readRecords + " result records " + resultRecords);
		System.out.println("# Execution time in sec " + stopwatch.elapsed(TimeUnit.SECONDS));
	}

	/**
	 * Execute a join
	 * @param args
	 * @throws InputParseException
	 */
	public void executeJoin(final String args[]) throws InputParseException {
		if(args.length != 7) {
			System.err.println("Usage: join <table1> <table2> <format1> <format2> <range> <padding>");
			System.exit(-1);
		}

		final Hyperrectangle hyperrectangle = Hyperrectangle.fromString(args[4]);

		final double padding = MathUtil.tryParseDouble(args[5], () -> "Unable to parse " + args[5]);

		executeJoin(args[1], args[2], args[3], args[4], hyperrectangle, padding);
	}

	/**
	 * Execute a join (nested loop join)
	 * @param table1
	 * @param table2
	 * @param range
	 */
	public void executeJoin(final String table1, final String table2, final String format1,
			final String format2, final Hyperrectangle range, final double padding) {

		System.out.println("# Execute join query in range " + range);

		final Stopwatch stopwatch = Stopwatch.createStarted();

		final TupleBuilder tupleBuilder1 = TupleBuilderFactory.getBuilderForFormat(format1);
	    final TupleBuilder tupleBuilder2 = TupleBuilderFactory.getBuilderForFormat(format2);

		long readRecords = 0;
		long resultRecords = 0;

		final SimpleStatement statementTable1 = new SimpleStatement("SELECT * FROM " + table1);
		statementTable1.setFetchSize(2000); // Use 2000 tuples per page

		final ResultSet rs1 = session.execute(statementTable1);

		for (final Row row1 : rs1) {

			// Request next page
		    if (rs1.getAvailableWithoutFetching() == 100 && !rs1.isFullyFetched()) {
		        rs1.fetchMoreResults(); // this is asynchronous
		    }

		    readRecords++;

		    final long id1 = row1.getLong(0);
		    final String text1 = row1.getString(1);

		    final Tuple tuple1 = tupleBuilder1.buildTuple(text1, Long.toString(id1));

		    // Tuple is outside of our query range
		    if(! tuple1.getBoundingBox().intersects(range)) {
		    	continue;
		    }

			// Perform the nested loop join
		    final SimpleStatement statementTable2 = new SimpleStatement("SELECT * FROM " + table2);
			statementTable2.setFetchSize(2000); // Use 2000 tuples per page

			final ResultSet rs2 = session.execute(statementTable2);

			for (final Row row2 : rs2) {
				// Request next page
			    if (rs1.getAvailableWithoutFetching() == 100 && !rs1.isFullyFetched()) {
			        rs1.fetchMoreResults(); // this is asynchronous
			    }

			    readRecords++;

			    final long id2 = row2.getLong(0);
			    final String text2 = row2.getString(1);

			    final Tuple tuple2 = tupleBuilder2.buildTuple(text2, Long.toString(id2));

			    if(tuple1.getBoundingBox().intersects(tuple2.getBoundingBox().enlargeByAmount(padding))) {
			    	resultRecords++;
			    }
			}
		}

		System.out.println("# Read records " + readRecords + " result records " + resultRecords);
		System.out.println("# Execution time in sec " + stopwatch.elapsed(TimeUnit.SECONDS));
	}


	/**
	 * Main
	 * @param args
	 */
	public static void main(final String[] args) throws Exception {

		final Cluster cluster = Cluster.builder()
				.addContactPoint("127.0.0.1")
				.build();

		final TestBaselineApproach testBaselineApproach = new TestBaselineApproach(cluster);

		if(args.length == 0) {
			System.err.println("Usage: <Class> <import|rangequery|join>");
			System.exit(-1);
		}

		final String command = args[0];

		switch(command) {
		case "import":
			testBaselineApproach.executeImport(args);
			break;
		case "rangequery":
			testBaselineApproach.executeRangeQuery(args);
			break;
		case "join":
			testBaselineApproach.executeRangeQuery(args);
			break;
		default:
			System.err.println("Unknown command: " + command);
			System.exit(-1);
		}

		testBaselineApproach.close();
	}

}
