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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.DoubleInterval;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.QueryPlanBuilder;
import org.bboxdb.storage.entity.JoinedTuple;

public class MultiContinuousQueryClient implements Runnable {

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
	 * The data range
	 */
	private final Hyperrectangle range;
	
	/**
	 * The percentage of the size of the query rectangle
	 */
	private final double percentage;
	
	/**
	 * The amount of parallel queries
	 */
	private final double parallelQueries;

	public MultiContinuousQueryClient(final String contactPoint, final String clusterName, 
			final String table, final Hyperrectangle range, final double percentage,
			final double parallelQueries) {
			
				this.contactPoint = contactPoint;
				this.clusterName = clusterName;
				this.table = table;
				this.range = range;
				this.percentage = percentage;
				this.parallelQueries = parallelQueries;
	}

	/**
	 * Determine a random query rectangle
	 * @return
	 */
	public Hyperrectangle getRandomQueryRange() {
		final List<DoubleInterval> bboxIntervals = new ArrayList<>();
		
		// Determine query bounding box
		for(int dimension = 0; dimension < range.getDimension(); dimension++) {
			final double dataExtent = range.getExtent(dimension);
			final double randomDouble = ThreadLocalRandom.current().nextDouble();
			final double bboxOffset = (randomDouble % 1) * dataExtent;
			final double coordinateLow = range.getCoordinateLow(dimension);
			final double coordinateHigh = range.getCoordinateHigh(dimension);

			final double bboxStartPos = coordinateLow + bboxOffset;
			
			final double queryExtend = dataExtent * percentage;
			final double bboxEndPos = Math.min(bboxStartPos + queryExtend, coordinateHigh);

			final DoubleInterval doubleInterval = new DoubleInterval(bboxStartPos, bboxEndPos);
			bboxIntervals.add(doubleInterval);
		}
		
		return new Hyperrectangle(bboxIntervals);
	}
	
	@Override
	public void run() {
		
		try(final BBoxDB connection = new BBoxDBCluster(contactPoint, clusterName)) {
			connection.connect();
			
			for(int i = 0; i < parallelQueries; i++) {
				final Hyperrectangle queryRectangle = getRandomQueryRange();
				System.out.println("Creating query in range: " + queryRectangle);
				
				final ContinuousQueryPlan queryPlan = QueryPlanBuilder
						.createQueryOnTable(table)
						.compareWithStaticRegion(queryRectangle)
						.forAllNewTuplesStoredInRegion(queryRectangle)
						.build();
				
				final JoinedTupleListFuture queryFuture = connection.queryContinuous(queryPlan);
				readResultsInThread(queryFuture);
			}
		} catch (BBoxDBException e) {
			e.printStackTrace();
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
				
				for(final JoinedTuple tuple : queryFuture) {
					tuple.getBoundingBox(); // Consume and ignore the tuple
				}
			} catch (InterruptedException e) {
				return;
			}
		});
		
		thread.start();
	}

	/**
	 * Main Main Main Main
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args.length != 6) {
			System.err.println("Usage: <Class> <ClusterContactPoint> <Clusterneme> <Table> <Range> <Percentage> <Parallel-Queries>");
			System.exit(-1);
		}
		
		final String contactPoint = args[0];
		final String clusterName = args[1];
		final String table = args[2];
		final String rangeString = args[3];
		final String percentageString = args[4];
		final String parallelQueriesString = args[5];
		
		
		final Optional<Hyperrectangle> range = HyperrectangleHelper.parseBBox(rangeString);
		
		if(! range.isPresent()) {
			System.err.println("Unable to parse as bounding box: " + rangeString);
		}
		
		final double percentage = MathUtil.tryParseDoubleOrExit(percentageString, () -> "Unable to parse: " + percentageString);
		final double parallelQueries = MathUtil.tryParseDoubleOrExit(parallelQueriesString, () -> "Unable to parse: " + parallelQueriesString);
		
		final MultiContinuousQueryClient runable = new MultiContinuousQueryClient(contactPoint, clusterName, 
				table, range.get(), percentage, parallelQueries);
		
		runable.run();
	}

}
