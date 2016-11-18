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

import java.io.Serializable;

public class OSMPoint implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7960562080700743356L;

	/**
	 * The x value
	 */
	protected double x;
	
	/**
	 * The y value
	 */
	protected double y;

	public OSMPoint(final double x, final double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	@Override
	public String toString() {
		return "OSMPoint [x=" + x + ", y=" + y + "]";
	}
	
}
