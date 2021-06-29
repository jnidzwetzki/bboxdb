/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.QueryPlanBuilder;
import org.bboxdb.storage.entity.MultiTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DemoLatLonEnlargementQuery implements Runnable {

	/**
	 * The cluster contact point
	 */
	private final String contactPoint;
	
	/**
	 * The name of the cluster
	 */
	private final String clusterName;
	
	/**
	 * The table to query
	 */
	private final String table;
	
	/**
	 * The latitude enlargement
	 */
	private final double enlargementLat;
	
	/**
	 * The longitude enlargement
	 */
	private final double enlargementLon;
	
	/**
	 * All threads
	 */
	private final List<Thread> allThreads = new CopyOnWriteArrayList<>();
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DemoLatLonEnlargementQuery.class);
	
	public DemoLatLonEnlargementQuery(final String contactPoint, final String clusterName, 
			final String table, final double enlargementLat, final double enlargementLon) {
			
				this.contactPoint = contactPoint;
				this.clusterName = clusterName;
				this.table = table;
				this.enlargementLat = enlargementLat;
				this.enlargementLon = enlargementLon;
	}
	
	@Override
	public void run() {
		
		try(final BBoxDB connection = new BBoxDBCluster(contactPoint, clusterName)) {
			connection.connect();
			
			final Hyperrectangle queryRectangle = new Hyperrectangle(1.0, 2.0, 1.0, 2.0);
			System.out.println("Creating query in range: " + queryRectangle);
			
			final ContinuousQueryPlan queryPlan = QueryPlanBuilder
					.createQueryOnTable(table)
					.forAllNewTuplesInSpace(queryRectangle)
					.enlargeStreamTupleBoundBoxByWGS84Meter(enlargementLat, enlargementLon)
					.compareWithStaticSpace(queryRectangle)
					.build();
			
			final JoinedTupleListFuture queryFuture = connection.queryContinuous(queryPlan);
			readResultsInThread(queryFuture);
						
			for(final Thread thread : allThreads) {
				thread.join();
			}
		} catch (BBoxDBException e) {
			logger.error("Got an exception", e);
		} catch (InterruptedException e) {
			return;
		} 
	}
	
	/**
	 * 
	 * @param queryFuture
	 */
	private void readResultsInThread(final JoinedTupleListFuture queryFuture) {
		final Thread thread = new Thread(() -> {
			
			try {
				queryFuture.waitForCompletion();
				
				for(final MultiTuple tuple : queryFuture) {
					tuple.getBoundingBox(); // Consume and ignore the tuple
				}
			} catch (InterruptedException e) {
				return;
			}
		});
		
		allThreads.add(thread);
		thread.start();
	}

	/**
	 * Main Main Main Main
	 * @param args
	 * @throws InputParseException 
	 */
	public static void main(String[] args) throws InputParseException {
		
		if(args.length != 5) {
			System.err.println("Usage: <Class> <ClusterContactPoint> <Clusterneme> <Table> <Enlargement Lat> <Enlagement Lon>");
			System.exit(-1);
		}
		
		final String contactPoint = args[0];
		final String clusterName = args[1];
		final String table = args[2];
		final String enlargementLatString = args[3];
		final String enlargementLonString = args[4];
		
		final double enlargementLat = MathUtil.tryParseDouble(enlargementLatString, () -> "Unable to parse: " + enlargementLatString);
		final double enlargementLon = MathUtil.tryParseDouble(enlargementLonString, () -> "Unable to parse: " + enlargementLonString);

		final DemoLatLonEnlargementQuery runable = new DemoLatLonEnlargementQuery(contactPoint, clusterName, 
				table, enlargementLat, enlargementLon);
		
		runable.run();
	}

}
