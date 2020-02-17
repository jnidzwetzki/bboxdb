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
package org.bboxdb.tools.network;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.bboxdb.commons.concurrent.ExceptionSafeRunnable;
import org.bboxdb.commons.math.GeoJsonPolygon;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexBuilder;
import org.bboxdb.storage.sstable.spatialindex.SpatialIndexEntry;
import org.bboxdb.storage.sstable.spatialindex.rtree.RTreeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.transit.realtime.GtfsRealtime;
import com.google.transit.realtime.GtfsRealtime.FeedEntity;
import com.google.transit.realtime.GtfsRealtime.FeedMessage;
import com.google.transit.realtime.GtfsRealtime.Position;
import com.google.transit.realtime.GtfsRealtime.TripDescriptor;
import com.google.transit.realtime.GtfsRealtime.VehiclePosition;

public class FetchRunable extends ExceptionSafeRunnable {
	
	/**
	 * The polygon consumer
	 */
	private final Consumer<GeoJsonPolygon> consumer;

	/**
	 * The fetch delay
	 */
	private final long fetchDelay;
	
	/**
	 * The url for fetching
	 */
	private final String urlString;

	/**
	 * The auth key for fetching
	 */
	private final String authKey;
	
	/**
	 * The entity name for fetching
	 */
	private final String entityName;
	
	/**
	 * Removed duplicates
	 */
	private final boolean removeDuplicates;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(FetchRunable.class);

	public FetchRunable(final String urlString, final String authKey, 
			final Consumer<GeoJsonPolygon> consumer, final long fetchDelay, final String entityName, 
			final boolean removeDuplicates) {
		
		this.urlString = urlString;
		this.authKey = authKey;
		this.consumer = consumer;
		this.fetchDelay = fetchDelay;
		this.entityName = entityName;
		this.removeDuplicates = removeDuplicates;
	}

	/**
	 * Fetch and insert data
	 * 
	 * curl -X GET --header 'Accept: text/plain' --header 'Authorization: ' 'https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/buses'
	 * @param bboxdbClient 
	 */
	@Override
	protected void runThread() throws Exception {
		while(! Thread.currentThread().isInterrupted()) {
			try {
				final URL url = new URL(urlString);
				final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
				connection.setRequestMethod("GET");
				connection.setDoInput(true);
				connection.setRequestProperty("Accept", "text/plain");
				connection.setRequestProperty("Authorization", "apikey " + authKey);
				
				final FeedMessage message = GtfsRealtime.FeedMessage.parseFrom(connection.getInputStream());
				
				final List<GeoJsonPolygon> polygonList = parseElements(message);
				final int inserts = insertData(polygonList);
				logger.info("Inserted {} {} (read {})", inserts, entityName, polygonList.size());
			} catch (Exception e) {
				logger.error("Got exception", e);
			}
			
			Thread.sleep(fetchDelay);
		}
	}

	/**
	 * Parse the elements from stream
	 * @param message
	 * @return
	 */
	private List<GeoJsonPolygon> parseElements(final FeedMessage message) {
		final List<FeedEntity> entities = message.getEntityList();
		final List<GeoJsonPolygon> polygonList = new ArrayList<>();
		
		for(final GtfsRealtime.FeedEntity entity : entities) {
			final VehiclePosition vehicle = entity.getVehicle();
			final TripDescriptor trip = vehicle.getTrip();
			final Position position = vehicle.getPosition();

			final GeoJsonPolygon geoJsonPolygon = new GeoJsonPolygon(0);
			geoJsonPolygon.addProperty("UpdateTimestamp", Long.toString(vehicle.getTimestamp()));
			geoJsonPolygon.addProperty("ParsedTimestamp", Long.toString(System.currentTimeMillis()));
			geoJsonPolygon.addProperty("RouteID", trip.getRouteId());
			geoJsonPolygon.addProperty("TripID", trip.getTripId());
			geoJsonPolygon.addProperty("TripStartDate", trip.getStartDate());
			geoJsonPolygon.addProperty("TripStartTime", trip.getStartTime());
			geoJsonPolygon.addProperty("TripScheduleRelationship", trip.getScheduleRelationship().name());
			geoJsonPolygon.addProperty("Speed", Float.toString(position.getSpeed()));
			geoJsonPolygon.addProperty("Bearing", Float.toString(position.getBearing()));
			geoJsonPolygon.addProperty("DirectionID", Integer.toString(trip.getDirectionId()));
			geoJsonPolygon.addProperty("CongestionLevel", vehicle.getCongestionLevel().name());
			geoJsonPolygon.addProperty("OccupancyStatus", vehicle.getOccupancyStatus().name());
			geoJsonPolygon.addPoint(position.getLongitude(), position.getLatitude());
			
			polygonList.add(geoJsonPolygon);
		}
		
		return polygonList;
	}


	/**
	 * Insert the received tuples
	 * @param bboxdbClient
	 * @param table 
	 * @param polygonList
	 * @return
	 * @throws BBoxDBException
	 */
	private int insertData(final List<GeoJsonPolygon> polygonList) throws BBoxDBException {
		
		// Remove duplicates on same pos if needed
		if(! removeDuplicates) {
			polygonList.forEach(p -> consumer.accept(p));
			return polygonList.size();
		}
		
		return insertWithoutDuplicates(polygonList);
	}

	/**
	 * Insert the data without duplicates
	 * @param polygonList
	 * @return
	 */
	private int insertWithoutDuplicates(final List<GeoJsonPolygon> polygonList) {
		// Sort by id
		polygonList.sort((p1, p2) -> Long.compare(p1.getId(), p2.getId()));
		
		final SpatialIndexBuilder index = new RTreeBuilder();

		for(int i = 0; i < polygonList.size(); i++) {
			final GeoJsonPolygon polygon = polygonList.get(i);
			final SpatialIndexEntry spe = new SpatialIndexEntry(polygon.getBoundingBox(), i);
			index.insert(spe);
		}
					
		final Set<Integer> processedElements = new HashSet<>();
		int inserts = 0;
		
		for(int i = 0; i < polygonList.size(); i++) {
			
			if(processedElements.contains(i)) {
				continue;
			}
			
			final GeoJsonPolygon polygon = polygonList.get(i);
			
			// Merge entries
			final List<? extends SpatialIndexEntry> entries = index.getEntriesForRegion(polygon.getBoundingBox());
			for(SpatialIndexEntry entry : entries) {
				processedElements.add(entry.getValue());
			}
			
			consumer.accept(polygon);
			
			inserts++;
		}
		
		return inserts;
	}

}
