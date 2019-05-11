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

import javax.swing.JComponent;
import javax.swing.JPanel;

import org.bboxdb.tools.gui.GuiModel;
import org.bboxdb.tools.gui.views.View;

public class QueryView implements View {

	/**
	 * The GUI model
	 */
	private final GuiModel guiModel;

	public QueryView(final GuiModel guiModel) {
		this.guiModel = guiModel;
	}
	
	@Override
	public JComponent getJPanel() {
		return new JPanel();
	}

	@Override
	public boolean isGroupSelectionNeeded() {
		return false;
	}}
