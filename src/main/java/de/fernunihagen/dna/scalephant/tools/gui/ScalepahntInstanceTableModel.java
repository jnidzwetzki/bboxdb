package de.fernunihagen.dna.scalephant.tools.gui;

import java.util.List;

import javax.swing.table.AbstractTableModel;

import de.fernunihagen.dna.scalephant.distribution.membership.DistributedInstance;

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
			
			return "";
		}
	}

	@Override
	public int getRowCount() {
		return instances.size();
	}

	@Override
	public int getColumnCount() {
		return 4;
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
	   }
		
	   return "---";
	}

}