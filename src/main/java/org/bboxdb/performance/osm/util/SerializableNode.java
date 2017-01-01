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
package org.bboxdb.performance.osm.util;

import java.io.Serializable;
import java.util.Date;

import org.openstreetmap.osmosis.core.domain.v0_6.Node;

public class SerializableNode implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6245642248212079374L;

	/**
	 * The id
	 */
	protected final long id;
	
	/**
	 * The latitude
	 */
	protected final double latitude;
	
	/**
	 * The longitude
	 */
	protected final double longitude;

	/**
	 * The timestamp
	 */
	protected final Date timestamp;

	/**
	 * The version
	 */
	protected final int version;
	
	public SerializableNode(final Node node) {
		this.latitude = node.getLatitude();
		this.longitude = node.getLongitude();
		this.id = node.getId();
		this.timestamp = node.getTimestamp();
		this.version = node.getVersion();
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public long getId() {
		return id;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	public int getVersion() {
		return version;
	}

}
