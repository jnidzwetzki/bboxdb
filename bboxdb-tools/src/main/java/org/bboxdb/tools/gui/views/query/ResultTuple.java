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
package org.bboxdb.tools.gui.views.query;

import java.util.List;

public class ResultTuple {

	/**
	 * The elements of the result
	 */
	public final List<OverlayElement> elements;

	public ResultTuple(final List<OverlayElement> elements) {
		this.elements = elements;
	}
	
	public int getNumberOfTuples() {
		return elements.size();
	}
	
	public OverlayElement getOverlayForTuple(final int tupleId) {
		return elements.get(tupleId);
	}
	
}
