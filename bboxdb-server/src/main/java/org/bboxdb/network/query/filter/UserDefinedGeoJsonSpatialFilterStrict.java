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
package org.bboxdb.network.query.filter;

import org.bboxdb.storage.entity.Tuple;
import org.json.JSONObject;

import com.esri.core.geometry.MapOGCStructure;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.WktImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;

public class UserDefinedGeoJsonSpatialFilterStrict implements UserDefinedFilter {
	
	private OGCGeometry customGeomety = null;
	
	/**
	 * Perform a real filter based on the geometry of the data
	 */
	@Override
	public boolean filterTuple(final Tuple tuple, final byte[] customData) {
		
		// No custom geometry is passed
		if(customData == null) { 
			return true;
		}
		
		// Cache the custom geometry between method calls
		if(customGeomety == null) {
			final String customString = new String(customData);
			customGeomety = geoJoinToGeomety(customString);
		}
		
		final String geoJsonString = new String(tuple.getDataBytes());
		final OGCGeometry geometry = extractGeometry(geoJsonString);

        return geometry.intersects(customGeomety);
	}
	
	/**
	 * Perform a real join based on the geometry of the data
	 */
	@Override
	public boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, final byte[] customData) {
		
		final String geoJsonString1 = new String(tuple1.getDataBytes());
		final String geoJsonString2 = new String(tuple2.getDataBytes());

		// Full text search on string (if provided)
		if(customData != null) {
			final String customDataString = new String(customData);
			if(! geoJsonString1.contains(customDataString) && 
					! geoJsonString2.contains(customDataString)) {
				return false;
			}
		}
		
		final OGCGeometry geometry1 = extractGeometry(geoJsonString1);
		final OGCGeometry geometry2 = extractGeometry(geoJsonString2);
		
	    return geometry1.intersects(geometry2);
	}
	
	/**
	 * Extract the geometry from the tuple
	 * @param tuple
	 * @return
	 */
	private OGCGeometry extractGeometry(final String geoJsonString) {

		final JSONObject jsonObject = new JSONObject(geoJsonString);
		
		// Extract geometry (if exists)
		final JSONObject geometryObject = jsonObject.optJSONObject("geometry");
		
		if(geometryObject != null) {
			return geoJoinToGeomety(geometryObject.toString());
		}
		
		return geoJoinToGeomety(geoJsonString);
	}

	/**
	 * Convert the geojson element to a ESRI geometry
	 * @param jsonString
	 * @return
	 */
	private OGCGeometry geoJoinToGeomety(String jsonString) {
		final OperatorImportFromGeoJson op = (OperatorImportFromGeoJson) OperatorFactoryLocal
		        .getInstance().getOperator(Operator.Type.ImportFromGeoJson);
				
	    final MapOGCStructure structure = op.executeOGC(WktImportFlags.wktImportDefaults, jsonString, null);

	    return OGCGeometry.createFromOGCStructure(structure.m_ogcStructure,
	    		structure.m_spatialReference);
	}

}
