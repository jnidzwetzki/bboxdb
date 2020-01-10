package org.bboxdb.tools.network;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The data sources
 *
 */
public class AuTransportSources {
	public final static String API_ENDPOINT_BASE = "https://api.transport.nsw.gov.au/v1/gtfs/vehiclepos/";
	
	public final static String SYDNEYTRAINS = "trains";
	public final static String BUSES = "buses";
	public final static String FERRIES = "ferries";
	public final static String LIGHTRAIL = "lightrail";
	public final static String NSWTRAINS = "nswtrains";
	public final static String REGIOBUSES = "regionbuses";
	public final static String METRO = "metro";
	
	public final static Map<String, String> API_ENDPOINT = new HashMap<>();
	
	public final static String SUPPORTED_ENTITIES;
	
	static {
		API_ENDPOINT.put(SYDNEYTRAINS, API_ENDPOINT_BASE + "sydneytrains");
		API_ENDPOINT.put(BUSES, API_ENDPOINT_BASE + "buses");
		API_ENDPOINT.put(FERRIES, API_ENDPOINT_BASE + "ferries/sydneyferries");
		API_ENDPOINT.put(LIGHTRAIL, API_ENDPOINT_BASE + "lightrail");
		API_ENDPOINT.put(NSWTRAINS, API_ENDPOINT_BASE + "nswtrains");
		API_ENDPOINT.put(REGIOBUSES, API_ENDPOINT_BASE + "regionbuses");
		API_ENDPOINT.put(METRO, API_ENDPOINT_BASE + "metro");
		
		SUPPORTED_ENTITIES = API_ENDPOINT.keySet().stream().collect(Collectors.joining(",", "[", "]"));
	}
}
