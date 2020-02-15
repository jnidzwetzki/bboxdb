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
package org.bboxdb.tools.gui.views.query;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDB;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.network.query.ContinuousQueryPlan;
import org.bboxdb.network.query.QueryPlanBuilder;
import org.bboxdb.network.query.filter.UserDefinedGeoJsonSpatialFilter;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.gui.GuiModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class QueryWindow {
	
	/**
	 * The query types
	 */
	private static final String QUERY_NONE = "---";
	private static final String QUERY_JOIN = "Spatial Join";
	private static final String QUERY_RANGE_CONTINUOUS = "Continuous range query";
	private static final String QUERY_RANGE = "Range query";

	/**
	 * The main frame
	 */
	private JFrame mainframe;
	
	/**
	 * The gui model
	 */
	private final GuiModel guimodel;
	
	/**
	 * Selected range latitude begin
	 */
	private String selectedLatBegin = "-90";

	/**
	 * Selected range latitude end
	 */
	private String selectedLatEnd = "90";
	
	/**
	 * Selected range longitude begin
	 */
	private String selectedLongBegin = "-180";
	
	/**
	 * Selected range longitude end
	 */
	private String selectedLongEnd = "180";

	/**
	 * The data to draw
	 */
	private final ElementOverlayPainter painter;
	
	/**
	 * The color names for the dropdowns
	 */
	private final static String[] COLOR_NAMES = new String[] {"Red", "Green", 
			"Blue", "Yellow", "Orange", "Pink"};
	
	/**
	 * The color values
	 */
	private final static Color[] COLOR_VALUES = new Color[] {Color.GREEN, Color.RED,
			Color.BLUE, Color.YELLOW, Color.ORANGE, Color.PINK};
	
	/**
	 * The created background threads
	 */
	private List<Thread> backgroundThreads;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(QueryWindow.class);

	
	public QueryWindow(final GuiModel guimodel, final ElementOverlayPainter painter, 
			final List<Thread> backgroundThreads) {
		
		this.guimodel = guimodel;
		this.painter = painter;
		this.backgroundThreads = backgroundThreads;
	}

	public void show() {
		this.mainframe = new JFrame("BBoxDB - Execute query");
		
		final PanelBuilder builder = buildDialog();
		
		mainframe.add(builder.getPanel());
		mainframe.pack();
		mainframe.setLocationRelativeTo(null);
		mainframe.setVisible(true);
	}

	private PanelBuilder buildDialog() {

		final FormLayout layout = new FormLayout(
			    "right:pref, 3dlu, 100dlu, 10dlu, right:pref, 3dlu, 100dlu", 			// columns
			    "p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 9dlu, p");	// rows
		
		final PanelBuilder builder = new PanelBuilder(layout);
		builder.setDefaultDialogBorder();
		
		final List<String> allTables = guimodel.getAllTables();
		final String[] allTableArray = allTables.toArray(new String[0]);
		
		final CellConstraints cc = new CellConstraints();
		builder.addSeparator("Query", cc.xyw(1,  1, 3));
		builder.addLabel("Type", cc.xy (1,  3));
		final String[] queries = new String[] {QUERY_NONE, QUERY_RANGE, QUERY_RANGE_CONTINUOUS, QUERY_JOIN};
		final JComboBox<String> queryTypeBox = new JComboBox<>(queries);
		builder.add(queryTypeBox, cc.xy (3,  3));
		
		builder.addLabel("Table 1", cc.xy (1,  5));
		final JComboBox<String> table1Field = new JComboBox<>(allTableArray);
		table1Field.setEnabled(false);
		builder.add(table1Field, cc.xy (3,  5));
		
		builder.addLabel("Color for table 1", cc.xy (1,  7));		
		final JComboBox<String> table1ColorField = new JComboBox<>(COLOR_NAMES);
		table1ColorField.setSelectedItem(COLOR_NAMES[0]);
		table1ColorField.setEnabled(false);
		builder.add(table1ColorField, cc.xy (3,  7));

		builder.addLabel("Table 2", cc.xy (1,  9));
		final JComboBox<String> table2Field = new JComboBox<>(allTableArray);
		table2Field.setEnabled(false);
		builder.add(table2Field, cc.xy (3,  9));
		
		builder.addLabel("Color for table 2", cc.xy (1,  11));
		final JComboBox<String> table2ColorField = new JComboBox<>(COLOR_NAMES);
		table2ColorField.setSelectedItem(COLOR_NAMES[1]);
		table2ColorField.setEnabled(false);
		builder.add(table2ColorField, cc.xy (3, 11));
		
		builder.addSeparator("Parameter", cc.xyw(5,  1, 3));
		
		builder.addLabel("Longitude begin", cc.xy (5,  3));
		final JTextField longBegin = new JTextField();
		longBegin.setText(selectedLongBegin);
		builder.add(longBegin, cc.xy (7,  3));
		
		builder.addLabel("Longitude end", cc.xy (5,  5));
		final JTextField longEnd = new JTextField();
		longEnd.setText(selectedLongEnd);
		builder.add(longEnd, cc.xy (7,  5));
		
		builder.addLabel("Latitude begin", cc.xy (5,  7));
		final JTextField latBegin = new JTextField();
		latBegin.setText(selectedLatBegin);
		builder.add(latBegin, cc.xy (7,  7));
		
		builder.addLabel("Latitude end", cc.xy (5,  9));
		final JTextField latEnd = new JTextField();
		latEnd.setText(selectedLatEnd);
		builder.add(latEnd, cc.xy (7,  9));

		builder.addLabel("UDF Name", cc.xy (5,  11));
		final JTextField filterField = new JTextField();
		builder.add(filterField, cc.xy (7,  11));

		builder.addLabel("UDF Value", cc.xy (5,  13));
		final JTextField valueField = new JTextField();
		builder.add(valueField, cc.xy (7,  13));

		// Close
		final JButton closeButton = new JButton();
		closeButton.setText("Close");
		closeButton.addActionListener((e) -> {
			mainframe.dispose();
		});
		
		final Action executeAction = getExecuteAction(queryTypeBox, 
				table1Field, table1ColorField, table2Field, table2ColorField, 
				longBegin, longEnd, latBegin, latEnd, filterField, valueField);
		
		final JButton executeButton = new JButton(executeAction);
		executeButton.setText("Execute");
		executeButton.setEnabled(false);
		
		addActionListener(queryTypeBox, table1Field,  table1ColorField, table2Field, 
				table2ColorField, executeButton, filterField);

		builder.add(closeButton, cc.xy(5, 17));
		builder.add(executeButton, cc.xy(7, 17));
		
		return builder;
	}

	/**
	 * Add the drop down action listener
	 * @param queryTypeBox
	 * @param table1Field
	 * @param table2Field
	 * @param executeButton 
	 */
	private void addActionListener(final JComboBox<String> queryTypeBox, final JComponent table1Field,
			final JComponent table1ColorField, final JComponent table2Field, final JComponent table2ColorField, 
			final JButton executeButton, JTextField filterField) {
		
		queryTypeBox.addActionListener(l -> {
			
			final String selectedQuery = queryTypeBox.getSelectedItem().toString();
			switch (selectedQuery) {
			
			case QUERY_RANGE:
			case QUERY_RANGE_CONTINUOUS:
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				executeButton.setEnabled(true);
				filterField.setText("");
				break;
				
			case QUERY_JOIN:
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				executeButton.setEnabled(true);
				filterField.setText(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName());
				break;

			default:
				table1Field.setEnabled(false);
				table1ColorField.setEnabled(false);
				table2Field.setEnabled(false);
				table1ColorField.setEnabled(false);
				executeButton.setEnabled(false);
				break;
			}
		});
		
	}

	/**
	 * Get the execute action
	 * @param valueField 
	 * @param filterField 
	 * @param rangeField 
	 * @param table2Field 
	 * @param table1Field 
	 * @param queryTypeBox 
	 * @return
	 */
	private Action getExecuteAction(final JComboBox<String> queryTypeBox, 
			final JComboBox<String> table1Field, final JComboBox<String> color1Box, 
			final JComboBox<String> table2Field, final JComboBox<String> color2Box, 
			final JTextField longBegin, final JTextField longEnd, final JTextField latBegin, 
			final JTextField latEnd, final JTextField filterField, final JTextField valueField) {
		
		final AbstractAction ececuteAction = new AbstractAction() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 6307940821474247538L;

			@Override
			public void actionPerformed(final ActionEvent e) {
				
				try {
					final String queryType = queryTypeBox.getSelectedItem().toString();
					final String table1 = table1Field.getSelectedItem().toString();
					final String table2 = table2Field.getSelectedItem().toString();
					final String filter = filterField.getText();
					final String value = valueField.getText();
					
					final double longBeginDouble = MathUtil.tryParseDouble(longBegin.getText(), () -> "Unable to parse value");
					final double longEndDouble = MathUtil.tryParseDouble(longEnd.getText(), () -> "Unable to parse value");
					final double latBeginDouble = MathUtil.tryParseDouble(latBegin.getText(), () -> "Unable to parse value");
					final double latEndDouble = MathUtil.tryParseDouble(latEnd.getText(), () -> "Unable to parse value");

					final Hyperrectangle resultBox = new Hyperrectangle(latBeginDouble, latEndDouble,
							longBeginDouble, longEndDouble);
									
					final Color color1 = COLOR_VALUES[color1Box.getSelectedIndex()];
					final Color color2 = COLOR_VALUES[color2Box.getSelectedIndex()];

					switch (queryType) {
					case QUERY_RANGE:
						executeRangeQuery(resultBox, table1, color1, filter, value);
						break;
					
					case QUERY_RANGE_CONTINUOUS:
						executeRangeQueryContinuous(resultBox, table1, color1, filter, value);
						break;
						
					case QUERY_JOIN:
						executeJoinQuery(resultBox, table1, color1, table2, color2, filter, value);
						break;

					default:
						throw new IllegalArgumentException("Unknown action: " + queryType);
					}				
					
					mainframe.dispose();
				} catch (InputParseException exception) {
					JOptionPane.showMessageDialog(mainframe, "Got an error: " + exception.getMessage());
				}
			}

			/***
			 * Execute a join query
			 * @param customValue 
			 * @param customFilter 
			 * @param table2 
			 * @param table1 
			 * @param bbox 
			 */
			private void executeJoinQuery(final Hyperrectangle bbox, final String table1, 
					final Color color1, final String table2, final Color color2, 
					final String customFilter, final String customValue) {
				
				try {
					final JoinedTupleListFuture result = guimodel.getConnection().queryJoin(
							Arrays.asList(table1, table2), bbox, customFilter, customValue.getBytes());
					
					result.waitForCompletion();
					if(result.isFailed()) {
						logger.error("Got an error" + result.getAllMessages());
						return;
					}
					
					final List<OverlayElementGroup> elements = new ArrayList<>();
					
					for(final JoinedTuple joinedTuple : result) {
						// Handle tuple0
						final Tuple tuple0 = joinedTuple.getTuple(0);
						final OverlayElement overlayElement0 = OverlayElementHelper.getOverlayElement(tuple0, table1, color1);

						// Handle tuple1
						final Tuple tuple1 = joinedTuple.getTuple(1);
						final OverlayElement overlayElement1 = OverlayElementHelper.getOverlayElement(tuple1, table2, color2);
							
						final List<OverlayElement> tupleList = Arrays.asList(overlayElement0, overlayElement1);
						final OverlayElementGroup resultTuple = new OverlayElementGroup(tupleList);
						elements.add(resultTuple);
					}
					
					painter.addElementToDrawBulk(elements);
					
				} catch (BBoxDBException e) {
					logger.error("Got error while performing query", e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				} 
			}

			/**
			 * Execute a range query
			 * @param customValue 
			 * @param customFilter 
			 * @param table 
			 * @param bbox 
			 */
			private void executeRangeQuery(final Hyperrectangle bbox, final String table, 
					final Color color, final String customFilter, final String customValue) {
				
				try {					
					final TupleListFuture result = guimodel.getConnection().queryRectangle(
							table, bbox, customFilter, customValue.getBytes());
					
					result.waitForCompletion();
					
					if(result.isFailed()) {
						logger.error("Got an error" + result.getAllMessages());
						return;
					}
					
					final List<OverlayElementGroup> elements = new ArrayList<>();
					
					for(final Tuple tuple : result) {
						final OverlayElement overlayElement = OverlayElementHelper.getOverlayElement(tuple, table, color);
						final OverlayElementGroup resultTuple = new OverlayElementGroup(Arrays.asList(overlayElement));
						elements.add(resultTuple);
					}
					
					logger.info("Got {} tuples back", elements.size());
					painter.addElementToDrawBulk(elements);
					
				} catch (BBoxDBException e) {
					logger.error("Got error while performing query", e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}

			/**
			 * Execute a continuous range query
			 * @param customValue 
			 * @param customFilter 
			 * @param table 
			 * @param bbox 
			 */
			private void executeRangeQueryContinuous(final Hyperrectangle bbox, final String table, 
					final Color color, final String customFilter, final String customValue) {
							
				final ContinuousQueryPlan qp = QueryPlanBuilder
						.createQueryOnTable(table)
						.compareWithStaticRegion(bbox)
						.build();
				
				final BBoxDB connection = guimodel.getConnection();

				final Runnable runable = new ContinuousQueryRunable(table, color, qp, connection, painter);
				
				final Thread fetchThread = new Thread(runable);
				backgroundThreads.add(fetchThread);
				fetchThread.start();
			}
		};
		
		return ececuteAction;
	}
	
	/**
	 * Set the latitude begin coordinate
	 * @param selectedLatBegin
	 */
	public void setSelectedLatBegin(final String selectedLatBegin) {
		this.selectedLatBegin = selectedLatBegin;
	}

	/**
	 * Set the latitude end coordinate
	 * @param selectedLatEnd
	 */
	public void setSelectedLatEnd(final String selectedLatEnd) {
		this.selectedLatEnd = selectedLatEnd;
	}

	/**
	 * Set the longitude begin coordinate
	 * @param selectedLongBegin
	 */
	public void setSelectedLongBegin(final String selectedLongBegin) {
		this.selectedLongBegin = selectedLongBegin;
	}

	/**
	 * Set the longitude end coordinate
	 * @param selectedLongEnd
	 */
	public void setSelectedLongEnd(final String selectedLongEnd) {
		this.selectedLongEnd = selectedLongEnd;
	}

}
