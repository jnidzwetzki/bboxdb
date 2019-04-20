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
package org.bboxdb.network.query.filter;

import org.bboxdb.storage.entity.Tuple;

import com.esri.core.geometry.MapOGCStructure;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.WktImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;

public class UserDefinedGeoJsonSpatialFilter implements UserDefinedFilter {

	@Override
	public boolean filterTuple(final Tuple tuple, String customData) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Perform a real 
	 */
	@Override
	public boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, final String customData) {
		final OperatorImportFromGeoJson op = (OperatorImportFromGeoJson) OperatorFactoryLocal
		        .getInstance().getOperator(Operator.Type.ImportFromGeoJson);
		
		final String geoJson1 = new String(tuple1.getDataBytes());
		final String geoJson2 = new String(tuple2.getDataBytes());
		
	    final MapOGCStructure structure1 = op.executeOGC(WktImportFlags.wktImportDefaults, geoJson1, null);
	    final MapOGCStructure structure2 = op.executeOGC(WktImportFlags.wktImportDefaults, geoJson2, null);

	    final OGCGeometry geometry1 = OGCGeometry.createFromOGCStructure(structure1.m_ogcStructure,
	    		structure1.m_spatialReference);
	    
	    final OGCGeometry geometry2 = OGCGeometry.createFromOGCStructure(structure2.m_ogcStructure,
	    		structure1.m_spatialReference);
	    
	    return geometry1.intersects(geometry2);
	}

}
