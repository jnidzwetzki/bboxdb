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
package org.bboxdb.query.filter;

import org.bboxdb.storage.entity.Tuple;

import com.esri.core.geometry.MapOGCStructure;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorImportFromGeoJson;
import com.esri.core.geometry.WktImportFlags;
import com.esri.core.geometry.ogc.OGCGeometry;

public class SimpleUserDefinedGeoJSONFilter implements UserDefinedFilter {

	public boolean filterJoinCandidate(final Tuple tuple1, final Tuple tuple2, final byte[] customData) {
        final OGCGeometry geo1 = extractGeometry(tuple1);
        final OGCGeometry geo2 = extractGeometry(tuple2);
        return geo1.intersects(geo2);
	}
	
	private OGCGeometry extractGeometry(final Tuple tuple) {
       final String jsonString = new String(tuple.getDataBytes());
		
       final OperatorImportFromGeoJson op = (OperatorImportFromGeoJson) OperatorFactoryLocal.getInstance().getOperator(Operator.Type.ImportFromGeoJson);
				
       final MapOGCStructure structure = op.executeOGC(WktImportFlags.wktImportDefaults, jsonString, null);

       return OGCGeometry.createFromOGCStructure(structure.m_ogcStructure, structure.m_spatialReference);
	}
}