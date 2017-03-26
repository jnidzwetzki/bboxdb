/*******************************************************************************
 *
 *    Copyright (C) 2015-2017 the BBoxDB project
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
package org.bboxdb.performance.osm;

import java.util.Map;

import org.bboxdb.performance.osm.filter.singlepoint.OSMSinglePointEntityFilter;
import org.bboxdb.performance.osm.util.Polygon;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

public class OSMSinglePointSink implements Sink {

	/**
	 * The entity filter
	 */
	private final OSMSinglePointEntityFilter entityFilter;

	/**
	 * The structure callback
	 */
	protected OSMStructureCallback structureCallback;

	protected OSMSinglePointSink(final OSMSinglePointEntityFilter entityFilter, 
			final OSMStructureCallback structureCallback) {
		this.entityFilter = entityFilter;
		this.structureCallback = structureCallback;
	}

	@Override
	public void release() {
	}

	@Override
	public void complete() {
	}

	@Override
	public void initialize(final Map<String, Object> arg0) {
	}

	@Override
	public void process(final EntityContainer entityContainer) {
		
		if(entityContainer.getEntity() instanceof Node) {
			final Node node = (Node) entityContainer.getEntity();						
			
			if(entityFilter.forwardNode(node)) {
				final Polygon geometricalStructure = new Polygon(node.getId());
				geometricalStructure.addPoint(node.getLatitude(), node.getLongitude());
				structureCallback.processStructure(geometricalStructure);
			}
		}
	}
}