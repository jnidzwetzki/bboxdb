/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
	public final static String METRO = "metro";
	
	public final static Map<String, String> API_ENDPOINT = new HashMap<>();
	
	public final static String SUPPORTED_ENTITIES;
	
	static {
		API_ENDPOINT.put(SYDNEYTRAINS, API_ENDPOINT_BASE + "sydneytrains");
		API_ENDPOINT.put(BUSES, API_ENDPOINT_BASE + "buses");
		API_ENDPOINT.put(FERRIES, API_ENDPOINT_BASE + "ferries/sydneyferries");
		API_ENDPOINT.put(LIGHTRAIL, API_ENDPOINT_BASE + "lightrail");
		API_ENDPOINT.put(NSWTRAINS, API_ENDPOINT_BASE + "nswtrains");
		API_ENDPOINT.put(METRO, API_ENDPOINT_BASE + "metro");
		
		SUPPORTED_ENTITIES = API_ENDPOINT.keySet().stream().collect(Collectors.joining(",", "[", "]"));
	}
}
