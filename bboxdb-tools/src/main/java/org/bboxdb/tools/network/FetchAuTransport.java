/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.tools.network;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.EmptyResultFuture;
import org.bboxdb.network.client.tools.FixedSizeFutureStore;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.tuple.GeoJSONTupleBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;

public class FetchAuTransport implements Runnable {

	/**
	 * The connection endpoint
	 */
	private final static String ENDPOINT = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses";
	
	/**
	 * The amount of pending insert futures
	 */
	private final static int MAX_PENDING_FUTURES = 1000;
	
	/**
	 * The polling delay
	 */
	private long delay;
	
	/**
	 * The Auth key
	 */
	private final String authKey;
	
	/**
	 * The connection point
	 */
	private final String connectionPoint;
	
	/**
	 * The cluster name
	 */
	private final String clustername;
	
	/**
	 * The table
	 */
	private final String table;
	
	/**
	 * The pending futures
	 */
	private final FixedSizeFutureStore pendingFutures;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(FetchAuTransport.class);

	

	
	public FetchAuTransport(final String authKey, final String connectionPoint, 
			final String clustername, final String table, final int delay) {
				this.authKey = authKey;
				this.connectionPoint = connectionPoint;
				this.clustername = clustername;
				this.table = table;
				this.delay = TimeUnit.SECONDS.toMillis(delay);
				this.pendingFutures = new FixedSizeFutureStore(MAX_PENDING_FUTURES);
	}

	@Override
	public void run() {
		try (
	    		final BBoxDB bboxdbClient = new BBoxDBCluster(connectionPoint, clustername);
	        ){
			bboxdbClient.connect();
			fetchData(bboxdbClient);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			waitForPendingFutures();
		}   
	}
	
	/**
	 * Fetch and insert data
	 * 
	 * curl -X GET --header 'Accept: text/plain' --header 'Authorization: ' 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses'
	 * @param bboxdbClient 
	 */
	private void fetchData(final BBoxDB bboxdbClient) throws Exception {

		while(! Thread.currentThread().isInterrupted()) {
			final URL url = new URL(ENDPOINT);
			final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("GET");
			connection.setDoInput(true);
			connection.setRequestProperty("Accept", "text/plain");
			connection.setRequestProperty("Authorization", "apikey " + authKey);
			
			final FeedMessage message = GtfsRealtime.FeedMessage.parseFrom(connection.getInputStream());
			
			final List<FeedEntity> entities = message.getEntityList();
			
			for(final GtfsRealtime.FeedEntity entity : entities) {
				final VehiclePosition vehicle = entity.getVehicle();
				final TripDescriptor trip = vehicle.getTrip();
				final Position position = vehicle.getPosition();

				final String idString = trip.getTripId().replace("_", "");
				final Optional<Long> id = MathUtil.tryParseLong(idString);
				
				if(! id.isPresent()) {
					logger.warn("Skipping element with invalid id: {}", idString);
					continue;
				}
				
				final GeoJsonPolygon geoJsonPolygon = new GeoJsonPolygon(id.get());
				
				geoJsonPolygon.addProperty("RouteID", trip.getRouteId());
				geoJsonPolygon.addProperty("TripID", trip.getTripId());
				geoJsonPolygon.addProperty("Speed", Float.toString(position.getSpeed()));
				geoJsonPolygon.addProperty("Bearing", Float.toString(position.getBearing()));
				geoJsonPolygon.addProperty("DirectionID", Integer.toString(trip.getDirectionId()));
				geoJsonPolygon.addPoint(position.getLongitude(), position.getLatitude());
				
				final GeoJSONTupleBuilder tupleBuilder = new GeoJSONTupleBuilder();
				final Tuple tuple = tupleBuilder.buildTuple(idString, geoJsonPolygon.toGeoJson());
				
				final EmptyResultFuture insertFuture = bboxdbClient.insertTuple(table, tuple);
				pendingFutures.put(insertFuture);
			}
			
			System.out.format("Inserted %d elements %n", entities.size());
			
			Thread.sleep(delay);
		}
	}

	/**
	 * Wait for the pending futures
	 */
	private void waitForPendingFutures() {
		try {
			pendingFutures.waitForCompletion();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	/**
	 * Main * Main * Main * Main * Main
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		if(args.length != 5) {
			System.err.println("Usage: <Class> <AuthKey> <Connection Endpoint> <Clustername> <Table> <Delay in sec>");
			System.exit(-1);
		}
		
		final String authKey = args[0];
		final String connectionPoint = args[1];
		final String clustername = args[2];
		final String table = args[3];
		final int delay = MathUtil.tryParseIntOrExit(args[4], () -> "Unable to parse delay value: " + args[4]);
				
		final FetchAuTransport main = new FetchAuTransport(authKey, connectionPoint, clustername, table, delay);
		main.run();
	}
}
