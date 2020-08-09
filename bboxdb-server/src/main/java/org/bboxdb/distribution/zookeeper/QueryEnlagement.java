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
package org.bboxdb.distribution.zookeeper;

public class QueryEnlagement {
	
	/**
	 * The max enlargement factor
	 */
	private double maxEnlargementFactor = 0;
	
	/**
	 * The max absolute enlargement
	 */
	private double maxAbsoluteEnlargement = 0;
	
	/**
	 * Max enlargement lat in meter
	 */
	private double maxEnlargementLat = 0;
	
	/**
	 * Max enlargement lon in meter
	 */
	private double maxEnlargementLon = 0;


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(maxAbsoluteEnlargement);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(maxEnlargementFactor);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(maxEnlargementLat);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(maxEnlargementLon);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryEnlagement other = (QueryEnlagement) obj;
		if (Double.doubleToLongBits(maxAbsoluteEnlargement) != Double.doubleToLongBits(other.maxAbsoluteEnlargement))
			return false;
		if (Double.doubleToLongBits(maxEnlargementFactor) != Double.doubleToLongBits(other.maxEnlargementFactor))
			return false;
		if (Double.doubleToLongBits(maxEnlargementLat) != Double.doubleToLongBits(other.maxEnlargementLat))
			return false;
		if (Double.doubleToLongBits(maxEnlargementLon) != Double.doubleToLongBits(other.maxEnlargementLon))
			return false;
		return true;
	}

	public double getMaxEnlargementFactor() {
		return maxEnlargementFactor;
	}

	public void setMaxEnlargementFactor(final double maxEnlargementFactor) {
		this.maxEnlargementFactor = maxEnlargementFactor;
	}

	public double getMaxAbsoluteEnlargement() {
		return maxAbsoluteEnlargement;
	}

	public void setMaxAbsoluteEnlargement(final double maxAbsoluteEnlargement) {
		this.maxAbsoluteEnlargement = maxAbsoluteEnlargement;
	}

	public double getMaxEnlargementLat() {
		return maxEnlargementLat;
	}

	public void setMaxEnlargementLat(final double maxEnlargementLat) {
		this.maxEnlargementLat = maxEnlargementLat;
	}

	public double getMaxEnlargementLon() {
		return maxEnlargementLon;
	}

	public void setMaxEnlargementLon(final double maxEnlargementLon) {
		this.maxEnlargementLon = maxEnlargementLon;
	}

	@Override
	public String toString() {
		return "QueryEnlagement [maxEnlargementFactor=" + maxEnlargementFactor + ", maxAbsoluteEnlargement="
				+ maxAbsoluteEnlargement + ", maxEnlargementLat=" + maxEnlargementLat + ", maxEnlargementLon="
				+ maxEnlargementLon + "]";
	}

}
