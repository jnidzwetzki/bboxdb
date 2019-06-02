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

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JTextField;

import org.bboxdb.commons.Pair;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.commons.math.HyperrectangleHelper;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.storage.entity.JoinedTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.converter.osm.util.Polygon;
import org.bboxdb.tools.gui.GuiModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class QueryWindow {
	
	/**
	 * The callback 
	 */
	private final Runnable callback;
	
	/**
	 * The main frame
	 */
	private JFrame mainframe;
	
	/**
	 * The gui model
	 */
	private final GuiModel guimodel;
	
	/**
	 * Selected range
	 */
	private String selectedRange = "";
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(QueryWindow.class);

	/**
	 * The data to draw
	 */
	private final Collection<Pair<List<Point2D>, Color>> dataToDraw;
	
	public QueryWindow(final GuiModel guimodel, final Collection<Pair<List<Point2D>, Color>> dataToDraw, 
			final Runnable callback) {
		this.guimodel = guimodel;
		this.dataToDraw = dataToDraw;
		this.callback = callback;
	}

	public void show() {
		mainframe = new JFrame("BBoxDB - Execute query");
		
		final PanelBuilder builder = buildDialog();
		
		mainframe.add(builder.getPanel());
		mainframe.pack();
		mainframe.setLocationRelativeTo(null);
		mainframe.setVisible(true);
	}

	private PanelBuilder buildDialog() {

		final FormLayout layout = new FormLayout(
			    "right:pref, 3dlu, 100dlu", 			// columns
			    "p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 9dlu, p");	// rows
		
		final PanelBuilder builder = new PanelBuilder(layout);
		builder.setDefaultDialogBorder();
		
		final List<String> allTables = guimodel.getAllTables();
		final String[] allTableArray = allTables.toArray(new String[0]);
		
		final CellConstraints cc = new CellConstraints();
		builder.addSeparator("Query", cc.xyw(1,  1, 3));
		builder.addLabel("Type", cc.xy (1,  3));
		final JComboBox<String> queryTypeBox = new JComboBox<>(new String[] {"---", "Range query", "Join"});
		builder.add(queryTypeBox, cc.xy (3,  3));
		
		builder.addLabel("Table 1", cc.xy (1,  5));
		final JComboBox<String> table1Field = new JComboBox<>(allTableArray);
		table1Field.setEnabled(false);
		builder.add(table1Field, cc.xy (3,  5));

		builder.addLabel("Table 2", cc.xy (1,  7));
		final JComboBox<String> table2Field = new JComboBox<>(allTableArray);
		table2Field.setEnabled(false);
		builder.add(table2Field, cc.xy (3,  7));
		
		builder.addLabel("Range", cc.xy (1,  9));
		final JTextField rangeField = new JTextField();
		rangeField.setText(selectedRange);
		builder.add(rangeField, cc.xy (3,  9));

		builder.addSeparator("Filter", cc.xyw(1,  11, 3));
		builder.addLabel("Name", cc.xy (1,  13));
		final JTextField filterField = new JTextField();
		builder.add(filterField, cc.xy (3,  13));

		builder.addLabel("Value", cc.xy (1,  15));
		final JTextField valueField = new JTextField();
		builder.add(valueField, cc.xy (3,  15));

		// Close
		final JButton closeButton = new JButton();
		closeButton.setText("Close");
		closeButton.addActionListener((e) -> {
			mainframe.dispose();
		});
		
		final Action executeAction = getExecuteAction(queryTypeBox, 
				table1Field, table2Field, rangeField, filterField, valueField);
		
		final JButton executeButton = new JButton(executeAction);
		executeButton.setText("Execute");
		executeButton.setEnabled(false);
		
		addActionListener(queryTypeBox, table1Field, table2Field, executeButton);

		
		builder.add(closeButton, cc.xy(1, 17));
		builder.add(executeButton, cc.xy(3, 17));
		
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
			final JComponent table2Field, final JButton executeButton) {
		
		queryTypeBox.addActionListener(l -> {
			
			final String selectedQuery = queryTypeBox.getSelectedItem().toString();
			switch (selectedQuery) {
			case "Range query":
				table1Field.setEnabled(true);
				table2Field.setEnabled(false);
				executeButton.setEnabled(true);
				break;
				
			case "Join":
				table1Field.setEnabled(true);
				table2Field.setEnabled(true);
				executeButton.setEnabled(true);
				break;

			default:
				table1Field.setEnabled(false);
				table2Field.setEnabled(false);
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
			final JComboBox<String> table1Field, final JComboBox<String> table2Field,
			final JTextField rangeFieldText, final JTextField filterField, final JTextField valueField) {
		
		final AbstractAction ececuteAction = new AbstractAction() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 6307940821474247538L;

			@Override
			public void actionPerformed(final ActionEvent e) {
				
				final String queryType = queryTypeBox.getSelectedItem().toString();
				final String table1 = table1Field.getSelectedItem().toString();
				final String table2 = table2Field.getSelectedItem().toString();
				final String range = rangeFieldText.getText();
				final String filter = filterField.getText();
				final String value = valueField.getText();
				
				final Optional<Hyperrectangle> resultBox = HyperrectangleHelper.parseBBox(range);
				if(! resultBox.isPresent()) {
					logger.error("Invalid bounding box: " + range);
					return;
				}
				
				final Hyperrectangle bbox = resultBox.get();
				
				switch (queryType) {
				case "Range query":
					executeRangeQuery(bbox, table1, filter, value);
					break;
					
				case "Join":
					executeJoinQuery(bbox, table1, table2, filter, value);
					break;

				default:
					throw new IllegalArgumentException("Unknown action: " + queryType);
				}				
				
				callback.run();
				mainframe.dispose();
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
					final String table2, final String customFilter, final String customValue) {
				
				try {
					final JoinedTupleListFuture result = guimodel.getConnection().queryJoin(
							Arrays.asList(table1, table2), bbox, customFilter, customValue.getBytes());
					
					result.waitForCompletion();
					if(result.isFailed()) {
						logger.error("Got an error" + result.getAllMessages());
						return;
					}
					
					for(final JoinedTuple tuple : result) {
						final String data1 = new String(tuple.getTuple(0).getDataBytes());
						final String data2 = new String(tuple.getTuple(1).getDataBytes());
					
						final Polygon polygon1 = Polygon.fromGeoJson(data1);
						addPolygon(polygon1, Color.GREEN);

						final Polygon polygon2 = Polygon.fromGeoJson(data2);
						addPolygon(polygon2, Color.RED);
						
						final Hyperrectangle bboxTuple = tuple.getBoundingBox();
						addBoundingBox(bboxTuple);
					}
					
				} catch (BBoxDBException e) {
					logger.error("Got error while performing query", e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}

			/**
			 * Add the polygon to the overlay
			 * @param polygon
			 * @param color 
			 */
			private void addPolygon(final Polygon polygon, final Color color) {
				final List<Point2D> polygonPoints = new ArrayList<>();

				for(final Point2D point : polygon.getPointList()) {
					polygonPoints.add(new Point2D.Double(point.getY(), point.getX())); 
				}

				dataToDraw.add(new Pair<>(polygonPoints, color));				
			}

			/**
			 * Add the bounding box to the overlay
			 * @param bboxTuple
			 */
			private void addBoundingBox(final Hyperrectangle bboxTuple) {
				final List<Point2D> boundingBoxPoints = new ArrayList<>();
				boundingBoxPoints.add(new Point2D.Double (bboxTuple.getCoordinateLow(1), bboxTuple.getCoordinateLow(0)));
				boundingBoxPoints.add(new Point2D.Double (bboxTuple.getCoordinateHigh(1), bboxTuple.getCoordinateLow(0)));
				boundingBoxPoints.add(new Point2D.Double (bboxTuple.getCoordinateHigh(1), bboxTuple.getCoordinateHigh(0)));
				boundingBoxPoints.add(new Point2D.Double (bboxTuple.getCoordinateLow(1), bboxTuple.getCoordinateHigh(0)));
				boundingBoxPoints.add(new Point2D.Double (bboxTuple.getCoordinateLow(1), bboxTuple.getCoordinateLow(0)));
				dataToDraw.add(new Pair<>(boundingBoxPoints, Color.BLACK));
			}

			/**
			 * Execute a range query
			 * @param customValue 
			 * @param customFilter 
			 * @param table1 
			 * @param bbox 
			 */
			private void executeRangeQuery(final Hyperrectangle bbox, final String table1, 
					final String customFilter, final String customValue) {
				
				try {
					final TupleListFuture result = guimodel.getConnection().queryRectangle(
							table1, bbox, customFilter, customValue.getBytes());
					
					result.waitForCompletion();
					if(result.isFailed()) {
						logger.error("Got an error" + result.getAllMessages());
						return;
					}
					
					for(final Tuple tuple : result) {
						final String data = new String(tuple.getDataBytes());
						final Polygon polygon = Polygon.fromGeoJson(data);
						addPolygon(polygon, Color.GREEN);

						final Hyperrectangle bboxTuple = tuple.getBoundingBox();
						addBoundingBox(bboxTuple);
					}
					
					
					System.out.println("Draw polygon " + dataToDraw.size());
					
					
				} catch (BBoxDBException e) {
					logger.error("Got error while performing query", e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}
		};
		
		return ececuteAction;
	}
	
	/**
	 * Set the selected range
	 * @param selectedRange
	 */
	public void setSelectedRange(final String selectedRange) {
		this.selectedRange = selectedRange;
	}
}
