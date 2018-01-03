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
package org.bboxdb.tools.gui;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

import org.bboxdb.distribution.membership.BBoxDBInstanceState;

public class InstanceTableRenderer extends DefaultTableCellRenderer {
	
	/**
	 * Color green
	 */
	private static final Color OUR_GREEN = new Color(0, 192, 0);

	/**
	 * Color gray
	 */
	private static final Color OUR_RED = new Color(164, 0, 0);
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -2405592763548531423L;

	@Override
	public Component getTableCellRendererComponent(JTable table,
			Object value, boolean isSelected, boolean hasFocus,
			int row, int column) {

		super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

		final String state = (String) table.getModel().getValueAt(row, 4);

		if(column != 4) {
			setForeground(table.getForeground());
		} else {
			if(BBoxDBInstanceState.OUTDATED.getZookeeperValue().equals(state)) {
				setForeground(Color.YELLOW);
			} else if(BBoxDBInstanceState.READY.getZookeeperValue().equals(state)) {
				setForeground(OUR_GREEN);
			} else {
				setForeground(OUR_RED);
			}
		}

		return this;
	}
}