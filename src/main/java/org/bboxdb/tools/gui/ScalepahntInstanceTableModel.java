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
package org.bboxdb.tools.gui;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import org.bboxdb.distribution.membership.DistributedInstance;

final class ScalepahntInstanceTableModel extends AbstractTableModel {

	private static final long serialVersionUID = 8593512480994197794L;


	/**
	 * The running scalephant instances
	 */
	protected final List<DistributedInstance> instances;
	
	public ScalepahntInstanceTableModel(final List<DistributedInstance> scalepahntInstances) {
		this.instances = scalepahntInstances;
	}

	/**
	 * Get table value for given position
	 * @param rowIndex
	 * @param columnIndex
	 * @return
	 */
	public Object getValueAt(int rowIndex, int columnIndex) {
		
		synchronized (instances) {
			
			// Is entry removed by another thread?
			if(rowIndex > instances.size()) {
				return "";
			}
			
			final DistributedInstance instance = instances.get(rowIndex);
		
			if(instances.size() < rowIndex) {
				return "";
			}
			
			if(instance == null) {
				return "";
			}
			
			if(columnIndex == 0) {
				return rowIndex;
			}
			
			if(columnIndex == 1) {
				return instance.getIp();
			}
			
			if(columnIndex == 2) {
				return instance.getPort();
			}
			
			if(columnIndex == 3) {
				return instance.getVersion();
			}
			
			if(columnIndex == 4) {
				return instance.getState().getZookeeperValue();
			}
			
			return "";
		}
	}

	@Override
	public int getRowCount() {
		return instances.size();
	}

	@Override
	public int getColumnCount() {
		return 5;
	}

	@Override
	public boolean isCellEditable(int rowIndex, int columnIndex) {
		return false;
	}

	@Override
	public String getColumnName(int column) {
	   
	   if(column == 0) {
		   return "Id";
	   } else if(column == 1) {
			return "IP";
	   } else if(column == 2) {
		   return "Port";
	   }  else if(column == 3) {
		   return "Version";
	   } else if(column == 4) {
		   return "State";
	   }
		
	   return "---";
	}

}