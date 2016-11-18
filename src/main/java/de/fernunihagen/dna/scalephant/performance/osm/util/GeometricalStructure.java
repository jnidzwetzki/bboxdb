/*******************************************************************************
 *
 *    Copyright (C) 2015-2016
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
package de.fernunihagen.dna.scalephant.performance.osm.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;

public class GeometricalStructure implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2119998223400835002L;
	
	/**
	 * The ID of the sttructure
	 */
	protected final long id;
	
	/**
	 * The list of our points
	 */
	protected List<OSMPoint> pointList = new ArrayList<OSMPoint>();
	
	public GeometricalStructure(final long id) {
		super();
		this.id = id;
	}

	public void addPoint(final double d, final double e) {
		final OSMPoint point = new OSMPoint(d, e);
		pointList.add(point);
	}
	
	/**
	 * Get the number of points
	 * @return 
	 */
	public int getNumberOfPoints() {
		return pointList.size();
	}
	
	/**
	 * Get the bounding box from the object
	 * @return
	 */
	public BoundingBox getBoundingBox() {
		
		if(pointList.isEmpty()) {
			return BoundingBox.EMPTY_BOX;
		}
		
		final OSMPoint firstPoint = pointList.get(0);
		double minX = firstPoint.getX();
		double maxX = firstPoint.getX();
		double minY = firstPoint.getY();
		double maxY = firstPoint.getY();
		
		for(final OSMPoint osmPoint : pointList) {
			minX = Math.min(minX, osmPoint.getX());
			maxX = Math.min(maxX, osmPoint.getX());
			minY = Math.min(minY, osmPoint.getY());
			maxY = Math.min(maxY, osmPoint.getY());
		}
		
		return new BoundingBox((float) minX, (float) maxX, (float) minY, (float) maxY);
	}

	/**
	 * Convert object to byte array
	 * @return
	 * @throws IOException 
	 */
	public byte[] toByteArray() throws IOException {
		final ByteArrayOutputStream bOutputStream = new ByteArrayOutputStream();
		final ObjectOutputStream objectOutputStream = new ObjectOutputStream(bOutputStream);

		objectOutputStream.writeObject(this);
		
		bOutputStream.close();
		return bOutputStream.toByteArray();
	}
	
	/**
	 * Construct object from byte array
	 * @param bytes
	 * @return
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public GeometricalStructure loadFromByteArray(final byte[] bytes) throws IOException, ClassNotFoundException {
		final ByteArrayInputStream bInputStream = new ByteArrayInputStream(bytes);
		final ObjectInputStream oInputStream = new ObjectInputStream(bInputStream);
		return (GeometricalStructure) oInputStream.readObject();
	}

	public long getId() {
		return id;
	}
	
}
