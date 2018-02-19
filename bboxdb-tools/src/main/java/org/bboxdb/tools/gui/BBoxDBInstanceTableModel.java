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

import java.util.List;

import javax.swing.table.AbstractTableModel;

import org.bboxdb.commons.FileSizeHelper;
import org.bboxdb.distribution.membership.BBoxDBInstance;

final class BBoxDBInstanceTableModel extends AbstractTableModel {

	private static final long serialVersionUID = 8593512480994197794L;
	
	/**
	 * The running bboxdb instances
	 */
	protected final List<BBoxDBInstance> instances;
	
	public BBoxDBInstanceTableModel(final List<BBoxDBInstance> distributedInstances) {
		this.instances = distributedInstances;
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
			
			final BBoxDBInstance instance = instances.get(rowIndex);
		
			if(instances.size() < rowIndex) {
				return "";
			}
			
			if(instance == null) {
				return "";
			}
			
			if(columnIndex == 0) {
				return rowIndex + 1;
			}
			
			if(columnIndex == 1) {
				
				if(GuiModel.SCREENSHOT_MODE) {
					return "XXX.XXX.XXX.XXX";
				}
				
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
			
			if(columnIndex == 5) {
				return instance.getCpuCores();
			}
			
			if(columnIndex == 6) {
				return FileSizeHelper.readableFileSize(instance.getMemory());
			}
			
			if(columnIndex == 7) {
				return instance.getNumberOfStorages();
			}
			
			if(columnIndex == 8) {
				return FileSizeHelper.readableFileSize(instance.getTotalSpace());
			}
			
			if(columnIndex == 9) {
				return FileSizeHelper.readableFileSize(instance.getFreeSpace());
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
		return 10;
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
	   } else if(column == 5) {
		   return "CPU cores";
	   } else if(column == 6) {
		   return "Memory";
	   } else if(column == 7) {
		   return "Storage locations";
	   } else if(column == 8) {
		   return "Total disk space";
	   } else if(column == 9) {
		   return "Free disk space";
	   }
		
	   return "---";
	}

}