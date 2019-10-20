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

import java.awt.BorderLayout;
import java.awt.Point;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.AbstractTableModel;

import org.bboxdb.commons.math.GeoJsonPolygon;
import org.jxmapviewer.JXMapViewer;

public class ResultDetailsWindow {
	
	/**
	 * The overlay painter
	 */
	private List<OverlayElement> renderedElements;
	
	/**
	 * The main frame
	 */
	private final JFrame mainframe;
	
	/**
	 * The table model to render
	 */
	private final AbstractTableModel tableModel;

	/**
	 * The map viewer
	 */
	private JXMapViewer mapViewer;
	
	public ResultDetailsWindow() {
		this.mainframe = new JFrame("BBoxDB - Query result view");
		this.tableModel = buildTableModel();
		final JPanel panel = buildDialog();
		
		this.mainframe.add(panel);
	}

	/**
	 * Build a new table model
	 * @return
	 */
	private AbstractTableModel buildTableModel() {
		
		final AbstractTableModel tableModel = new AbstractTableModel() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -5660979952458001100L;

			@Override
			public Object getValueAt(final int rowIndex, final int columnIndex) {
				
				final OverlayElement overlayElement = renderedElements.get(rowIndex);
				
				switch(columnIndex) {
					case 0:
						return overlayElement.getSourceTable();
					case 1:
						return overlayElement.getPolygon().getId();
					case 2: 
						return getKeyStringForOverlay(overlayElement);
					case 3:
						return overlayElement.getPolygon().toGeoJson();
					default:
						throw new IllegalArgumentException("Unable to get a column for: " + columnIndex);
				}
				
			}
			
			/**
			 * Build the table entry for the overlay element
			 * @param overlayElement
			 * @return
			 */
			private String getKeyStringForOverlay(final OverlayElement overlayElement) {
				final StringBuilder sb = new StringBuilder();
				
				final GeoJsonPolygon polygon = overlayElement.getPolygon();
				final Map<String, String> properties = polygon.getProperties();
				
				for(final Entry<String,String> entry : properties.entrySet()) {
					if(sb.length() != 0) {
						sb.append(", ");
					} else {
						sb.append("<html>");
					}
					
					sb.append("<b>" + entry.getKey() + "</b>");
					sb.append("=");
					sb.append(entry.getValue());
				}
				
				sb.append("</html>");
				return sb.toString();
			}

			@Override
			public int getRowCount() {
				return renderedElements.size();
			}
			
			@Override
			public int getColumnCount() {
				return 4;
			}
			
			@Override
			public String getColumnName(final int column) {
				switch(column) {
					case 0:
						return "Table";
					case 1:
						return "OSM-ID";
					case 2: 
						return "Tags";
					case 3:
						return "GeoJSON";
					default:
						throw new IllegalArgumentException("Unable to get a column name for: " + column);
				}
			}
		};
		
		return tableModel;
	}

	/**
	 * Make the windows visible
	 */
	public void show() {
		this.mainframe.pack();
		this.mainframe.setLocationRelativeTo(null);
		this.mainframe.setVisible(true);
	}

	/**
	 * Build the main dialog
	 * @return
	 */
	private JPanel buildDialog() {
		final JPanel mainPanel = new JPanel();
		mainPanel.setLayout(new BorderLayout());
		
		final JComponent jTable = buildJTable();
		mainPanel.add(jTable, BorderLayout.CENTER);
		
		final JButton closeButton = getCloseButton();
		mainPanel.add(closeButton, BorderLayout.SOUTH);
		
		return mainPanel;
	}

	/**
	 * Build a new JTable
	 * @return
	 */
	private JComponent buildJTable() {
		final JTable table = new JTable(tableModel);
		
		table.getSelectionModel().addListSelectionListener(l -> {
			if(l.getValueIsAdjusting()) {
				return;
			}
			
			for(final OverlayElement element : renderedElements) {
				element.setSelected(false);
			}
			
			final int[] selectedRows = table.getSelectedRows();
			
			for (int i = 0; i < selectedRows.length; i++) {
				final int selectedRow = selectedRows[i];
				renderedElements.get(selectedRow).setSelected(true);
			}
			
			mapViewer.repaint();
		});
		
		table.addMouseListener(new MouseAdapter() {
		    public void mousePressed(final MouseEvent mouseEvent) {
		        final JTable table =(JTable) mouseEvent.getSource();
		        final Point point = mouseEvent.getPoint();
		        final int row = table.rowAtPoint(point);
		        if (mouseEvent.getClickCount() == 2 && table.getSelectedRow() != -1) {
		        	final OverlayElement element = renderedElements.get(row);
		        	final DetailsWindow detailsWindow = new DetailsWindow(element.getPolygon());
		        	detailsWindow.show();
		        }
		    }
		});
		
		table.getColumnModel().getColumn(0).setMinWidth(140);
		table.getColumnModel().getColumn(0).setMaxWidth(140);
		table.getColumnModel().getColumn(1).setMinWidth(100);
		table.getColumnModel().getColumn(1).setMaxWidth(100);
		
		return new JScrollPane(table);
	}

	/**
	 * Get the close button
	 * @return
	 */
	private JButton getCloseButton() {
		final JButton closeButton = new JButton("Close");
		closeButton.addActionListener(l -> mainframe.setVisible(false));
		return closeButton;
	}

	/**
	 * Set new rendered elements
	 * @param renderedElements
	 */
	public void setRenderedElements(final List<OverlayElement> renderedElements) {
		this.renderedElements = renderedElements;
		
		if(mainframe.isVisible()) {
			this.tableModel.fireTableDataChanged();
		}
	}

	/**
	 * Set the map viewer
	 * @param mapViewer
	 */
	public void setMapViewer(JXMapViewer mapViewer) {
		this.mapViewer = mapViewer;		
	}

}
