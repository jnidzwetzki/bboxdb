package org.bboxdb.tools.gui;

import java.awt.Color;
import java.awt.Component;

import javax.swing.JTable;
import javax.swing.table.DefaultTableCellRenderer;

import org.bboxdb.distribution.membership.event.DistributedInstanceState;

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
			if(DistributedInstanceState.OUTDATED.getZookeeperValue().equals(state)) {
				setForeground(Color.YELLOW);
			} else if(DistributedInstanceState.READY.getZookeeperValue().equals(state)) {
				setForeground(OUR_GREEN);
			} else {
				setForeground(OUR_RED);
			}
		}

		return this;
	}
}