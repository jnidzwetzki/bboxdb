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
package org.bboxdb.commons;

public class Pair<S,T> {

	/**
	 * The first element
	 */
	private S element1;
	
	/**
	 * The second element
	 */
	private T element2;

	public Pair(final S element1, final T element2) {
		this.element1 = element1;
		this.element2 = element2;
	}

	/**
	 * Get the first element
	 * @return
	 */
	public S getElement1() {
		return element1;
	}

	/**
	 * Set the first element
	 * @param element1
	 */
	public void setElement1(final S element1) {
		this.element1 = element1;
	}

	/**
	 * Get the second element
	 * @return
	 */
	public T getElement2() {
		return element2;
	}

	/**
	 * Set the second element
	 */
	public void setElement2(final T element2) {
		this.element2 = element2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((element1 == null) ? 0 : element1.hashCode());
		result = prime * result + ((element2 == null) ? 0 : element2.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (element1 == null) {
			if (other.element1 != null)
				return false;
		} else if (!element1.equals(other.element1))
			return false;
		if (element2 == null) {
			if (other.element2 != null)
				return false;
		} else if (!element2.equals(other.element2))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Pair [element1=" + element1 + ", element2=" + element2 + "]";
	}

}
