/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
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
import java.util.concurrent.TimeUnit;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JTextField;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.math.Hyperrectangle;
import org.bboxdb.misc.BBoxDBException;
import org.bboxdb.network.client.BBoxDBCluster;
import org.bboxdb.network.client.future.client.JoinedTupleListFuture;
import org.bboxdb.network.client.future.client.TupleListFuture;
import org.bboxdb.query.ContinuousQueryPlan;
import org.bboxdb.query.QueryPlanBuilder;
import org.bboxdb.query.filter.UserDefinedFilterDefinition;
import org.bboxdb.query.filter.UserDefinedGeoJsonSpatialFilter;
import org.bboxdb.query.filter.UserDefinedGeoJsonSpatialFilterStrict;
import org.bboxdb.storage.entity.MultiTuple;
import org.bboxdb.storage.entity.Tuple;
import org.bboxdb.tools.gui.GuiModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.jgoodies.forms.builder.PanelBuilder;
import com.jgoodies.forms.layout.CellConstraints;
import com.jgoodies.forms.layout.FormLayout;

public class QueryWindow {
	
	/**
	 * The query types
	 */
	private static final String QUERY_NONE = "---";
	private static final String QUERY_RANGE = "Range query";
	private static final String QUERY_JOIN = "Spatial join";
	private static final String QUERY_RANGE_CONTINUOUS = "Continuous range query";
	private static final String QUERY_JOIN_CONTINUOUS = "Continuous spatial join";
	
	/**
	 * The predefined queries
	 */
	private static final String QUERY_PREDEFINED_NONE = "----";
	private static final String QUERY_PREDEFINED_STATIC_ROADS = "Roads (Static)";
	private static final String QUERY_PREDEFINED_STATIC_FOREST = "Forest (Static)";
	private static final String QUERY_PREDEFINED_AIRCRAFT = "Aircraft";
	private static final String QUERY_PREDEFINED_BUS = "Buses";
	private static final String QUERY_PREDEFINED_BUS_ROAD = "Buses joined with Road";
	private static final String QUERY_PREDEFINED_BUS_ELIZABETH = "Buses on Elizabeth Street";
	private static final String QUERY_PREDEFINED_BUS_BRIDGE = "Buses on a Bridge";
	private static final String QUERY_PREDEFINED_BUS_FOREST_BBOX = "Buses joined with Forest (bbox)";
	private static final String QUERY_PREDEFINED_BUS_FOREST_RELAXTED = "Buses joined with Forest (relaxed)";
	private static final String QUERY_PREDEFINED_BUS_FOREST_STRICT = "Buses joined with Forest (strict)";
	private static final String QUERY_PREDEFINED_STATIC_ROAD_VALUE = "Road with value";
	private static final String QUERY_PREDEFINED_STATIC_ROAD_FOREST_JOIN = "Road joined with Forest";

	
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
	private final static Color[] COLOR_VALUES = new Color[] {Color.RED, Color.GREEN,
			Color.BLUE, Color.YELLOW, Color.ORANGE, Color.PINK};
	
	/**
	 * True / False names
	 */
	private final static String[] BOOL_NAMES = new String[] {"Enabled", "Disabled"};
	
	/**
	 * True / False values
	 */
	private final static Boolean[] BOOL_VALUES = new Boolean[] {true, false};
	
	/**
	 * The predefined query values
	 */
	private final static String[] PREDEFINED_QUERIES = new String[] {QUERY_PREDEFINED_NONE, 
			QUERY_PREDEFINED_STATIC_ROADS, QUERY_PREDEFINED_STATIC_FOREST,
			QUERY_PREDEFINED_AIRCRAFT, QUERY_PREDEFINED_BUS, QUERY_PREDEFINED_BUS_ROAD, 
			QUERY_PREDEFINED_BUS_ELIZABETH, QUERY_PREDEFINED_BUS_BRIDGE, QUERY_PREDEFINED_BUS_FOREST_BBOX,
			QUERY_PREDEFINED_BUS_FOREST_RELAXTED, QUERY_PREDEFINED_BUS_FOREST_STRICT,
			QUERY_PREDEFINED_STATIC_ROAD_VALUE, QUERY_PREDEFINED_STATIC_ROAD_FOREST_JOIN};
	
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
			    "right:pref, 3dlu, 120dlu, 10dlu, right:pref, 3dlu, 100dlu", 			// columns
			    "p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 3dlu, p, 9dlu, p");	// rows
		
		final PanelBuilder builder = new PanelBuilder(layout);
		builder.setDefaultDialogBorder();
		
		final List<String> allTables = guimodel.getAllTables();
		final String[] allTableArray = allTables.toArray(new String[0]);
		
		final CellConstraints cc = new CellConstraints();
		builder.addSeparator("Query", cc.xyw(1,  1, 3));
		builder.addLabel("Type", cc.xy (1,  3));
		final String[] queries = new String[] {QUERY_NONE, QUERY_RANGE, QUERY_JOIN, 
				QUERY_RANGE_CONTINUOUS, QUERY_JOIN_CONTINUOUS};
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
		table2ColorField.setSelectedItem(COLOR_NAMES[2]);
		table2ColorField.setEnabled(false);
		builder.add(table2ColorField, cc.xy (3, 11));
	
		builder.addLabel("Receive watermarks", cc.xy (1,  13));
		final JComboBox<String> receiveWatermarksField = new JComboBox<>(BOOL_NAMES);
		receiveWatermarksField.setEnabled(false);
		builder.add(receiveWatermarksField, cc.xy (3, 13));
	
		builder.addLabel("Receive invalidations", cc.xy (1,  15));
		final JComboBox<String> receiveInvalidationsField = new JComboBox<>(BOOL_NAMES);
		receiveInvalidationsField.setEnabled(false);
		builder.add(receiveInvalidationsField, cc.xy (3, 15));
	
		builder.addSeparator("Predefined Queries", cc.xyw(1,  17, 3));
		builder.addLabel("Query", cc.xy (1,  19));
		final JComboBox<String> predefinedQueriesBox = new JComboBox<>(PREDEFINED_QUERIES);
		predefinedQueriesBox.setSelectedItem(PREDEFINED_QUERIES[0]);
		predefinedQueriesBox.setEnabled(true);
		builder.add(predefinedQueriesBox, cc.xy (3, 19));
		
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
		final JTextField udfNameField = new JTextField();
		builder.add(udfNameField, cc.xy (7,  11));

		builder.addLabel("UDF Value", cc.xy (5,  13));
		final JTextField udfValueField = new JTextField();
		builder.add(udfValueField, cc.xy (7,  13));

		// Close
		final JButton closeButton = new JButton();
		closeButton.setText("Close");
		closeButton.addActionListener((e) -> {
			mainframe.dispose();
		});
		
		final Action executeAction = getExecuteAction(queryTypeBox, 
				table1Field, table1ColorField, table2Field, table2ColorField, 
				longBegin, longEnd, latBegin, latEnd, udfNameField, udfValueField, 
				receiveWatermarksField, receiveInvalidationsField);
		
		final JButton executeButton = new JButton(executeAction);
		executeButton.setText("Execute");
		executeButton.setEnabled(false);
		
		addActionListener(queryTypeBox, predefinedQueriesBox, 
				table1Field,  table1ColorField, table2Field, 
				table2ColorField, executeButton, udfNameField, udfValueField,
				receiveWatermarksField, receiveInvalidationsField);

		builder.add(closeButton, cc.xy(5, 21));
		builder.add(executeButton, cc.xy(7, 21));
		
		return builder;
	}

	/**
	 * Add the drop down action listener
	 * @param queryTypeBox
	 * @param table1Field
	 * @param table2Field
	 * @param executeButton 
	 */
	private void addActionListener(final JComboBox<String> queryTypeBox, final JComboBox<String> predefinedQueriesBox,
			final JComboBox<String> table1Field, final JComboBox<String> table1ColorField, 
			final JComboBox<String> table2Field, final JComboBox<String> table2ColorField, 
			final JButton executeButton, final JTextField udfNameField, final JTextField udfValueField,
			final JComboBox<String> receiveWatermarksField, final JComboBox<String> receiveInvalidationsField) {
		
		
		// The predefined queries
		predefinedQueriesBox.addActionListener(l -> {

			final String selectedPredefinedQuery = predefinedQueriesBox.getSelectedItem().toString();
			
			switch(selectedPredefinedQuery) {
			case QUERY_PREDEFINED_NONE:
				udfNameField.setText("");
				udfValueField.setText("");
				queryTypeBox.setSelectedItem(QUERY_RANGE);
				break;
			case QUERY_PREDEFINED_STATIC_ROADS:
				udfNameField.setText("");
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_roads");
				table1ColorField.setSelectedItem("Blue");
				queryTypeBox.setSelectedItem(QUERY_RANGE);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				receiveWatermarksField.setEnabled(false);
				receiveInvalidationsField.setEnabled(false);
				break;
			case QUERY_PREDEFINED_STATIC_ROAD_VALUE:
				udfNameField.setText(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName());
				udfValueField.setText("lanes:4");
				table1Field.setSelectedItem("osmgroup_roads");
				table1ColorField.setSelectedItem("Blue");
				queryTypeBox.setSelectedItem(QUERY_RANGE);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				receiveWatermarksField.setEnabled(false);
				receiveInvalidationsField.setEnabled(false);
				break;
			case QUERY_PREDEFINED_STATIC_ROAD_FOREST_JOIN:
				udfNameField.setText(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName());
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_roads");
				table2Field.setSelectedItem("osmgroup_forests");
				table1ColorField.setSelectedItem("Blue");
				table2ColorField.setSelectedItem("Green");
				queryTypeBox.setSelectedItem(QUERY_JOIN);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				receiveWatermarksField.setEnabled(false);
				receiveInvalidationsField.setEnabled(false);
				break;
			case QUERY_PREDEFINED_STATIC_FOREST:
				udfNameField.setText("");
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_forests");
				table1ColorField.setSelectedItem("Green");
				queryTypeBox.setSelectedItem(QUERY_RANGE);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				receiveWatermarksField.setEnabled(false);
				receiveInvalidationsField.setEnabled(false);
				break;
			case QUERY_PREDEFINED_AIRCRAFT:
				udfNameField.setText("");
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_adsb");
				queryTypeBox.setSelectedItem(QUERY_RANGE_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			case QUERY_PREDEFINED_BUS: 
				udfNameField.setText("");
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_buses");
				queryTypeBox.setSelectedItem(QUERY_RANGE_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				table2ColorField.setSelectedItem("Blue");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			case QUERY_PREDEFINED_BUS_ROAD:
				udfNameField.setText(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName());
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_buses");
				table2Field.setSelectedItem("osmgroup_roads");
				queryTypeBox.setSelectedItem(QUERY_JOIN_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				table2ColorField.setSelectedItem("Blue");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			case QUERY_PREDEFINED_BUS_ELIZABETH:
				udfNameField.setText(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName());
				udfValueField.setText("name:Elizabeth Street");
				table1Field.setSelectedItem("osmgroup_buses");
				table2Field.setSelectedItem("osmgroup_roads");
				queryTypeBox.setSelectedItem(QUERY_JOIN_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				table2ColorField.setSelectedItem("Blue");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			case QUERY_PREDEFINED_BUS_BRIDGE:
				udfNameField.setText(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName());
				udfValueField.setText("bridge:yes");
				table1Field.setSelectedItem("osmgroup_buses");
				table2Field.setSelectedItem("osmgroup_roads");
				queryTypeBox.setSelectedItem(QUERY_JOIN_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				table2ColorField.setSelectedItem("Blue");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			case QUERY_PREDEFINED_BUS_FOREST_BBOX:
				udfNameField.setText("");
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_buses");
				table2Field.setSelectedItem("osmgroup_forests");
				queryTypeBox.setSelectedItem(QUERY_JOIN_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				table2ColorField.setSelectedItem("Green");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			case QUERY_PREDEFINED_BUS_FOREST_RELAXTED:
				udfNameField.setText(UserDefinedGeoJsonSpatialFilter.class.getCanonicalName());
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_buses");
				table2Field.setSelectedItem("osmgroup_forests");
				queryTypeBox.setSelectedItem(QUERY_JOIN_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				table2ColorField.setSelectedItem("Green");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			case QUERY_PREDEFINED_BUS_FOREST_STRICT:
				udfNameField.setText(UserDefinedGeoJsonSpatialFilterStrict.class.getCanonicalName());
				udfValueField.setText("");
				table1Field.setSelectedItem("osmgroup_buses");
				table2Field.setSelectedItem("osmgroup_forests");
				queryTypeBox.setSelectedItem(QUERY_JOIN_CONTINUOUS);
				executeButton.setEnabled(true);
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				table2ColorField.setSelectedItem("Green");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
			default:
				logger.error("Unknown selection: " + selectedPredefinedQuery);
			}
	
		});
		
		// The normal queries
		queryTypeBox.addActionListener(l -> {
			
			// Are predefined queries in control?
			if(! predefinedQueriesBox.getSelectedItem().toString().equals(QUERY_PREDEFINED_NONE)) {
				return;
			}
			
			final String selectedQuery = queryTypeBox.getSelectedItem().toString();
			
			switch (selectedQuery) {
			
			case QUERY_RANGE:
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				executeButton.setEnabled(true);
				udfNameField.setText("");
				udfValueField.setText("");
				receiveWatermarksField.setEnabled(false);
				receiveInvalidationsField.setEnabled(false);
				break;
				
			case QUERY_RANGE_CONTINUOUS:
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(false);
				table2ColorField.setEnabled(false);
				executeButton.setEnabled(true);
				udfNameField.setText("");
				udfValueField.setText("");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;
				
			case QUERY_JOIN:
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				executeButton.setEnabled(true);
				udfNameField.setText("");
				udfValueField.setText("");
				receiveWatermarksField.setEnabled(false);
				receiveInvalidationsField.setEnabled(false);
				break;
				
			case QUERY_JOIN_CONTINUOUS:
				table1Field.setEnabled(true);
				table1ColorField.setEnabled(true);
				table2Field.setEnabled(true);
				table2ColorField.setEnabled(true);
				executeButton.setEnabled(true);
				udfNameField.setText("");
				udfValueField.setText("");
				receiveWatermarksField.setEnabled(true);
				receiveInvalidationsField.setEnabled(true);
				break;

			default:
				table1Field.setEnabled(false);
				table1ColorField.setEnabled(false);
				table2Field.setEnabled(false);
				table1ColorField.setEnabled(false);
				executeButton.setEnabled(false);
				udfNameField.setText("");
				udfValueField.setText("");
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
			final JTextField latEnd, final JTextField filterField, final JTextField valueField, 
			final JComboBox<String> receiveWatermarksField, final JComboBox<String> receiveInvalidationsField) {
		
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
					
					final boolean receiveWatermarks = BOOL_VALUES[receiveWatermarksField.getSelectedIndex()];
					final boolean receiveInvalidations = BOOL_VALUES[receiveInvalidationsField.getSelectedIndex()];
					
					switch (queryType) {
					case QUERY_RANGE:
						executeRangeQuery(resultBox, table1, color1, filter, value);
						break;
				
					case QUERY_JOIN:
						executeJoinQuery(resultBox, table1, color1, table2, color2, filter, value);
						break;
				
					case QUERY_RANGE_CONTINUOUS:
						executeRangeQueryContinuous(resultBox, table1, color1, filter, value, 
								receiveWatermarks, receiveInvalidations);
						break;
						
					case QUERY_JOIN_CONTINUOUS:
						executeJoinQueryContinuous(resultBox, table1, table2, color1, color2, 
								filter, value, receiveWatermarks, receiveInvalidations);
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
					
					final List<UserDefinedFilterDefinition> udfs = new ArrayList<>();
					
					if(customFilter.length() > 1) {
						final UserDefinedFilterDefinition udf = new UserDefinedFilterDefinition(customFilter, customValue);
						udfs.add(udf);
					}
					
					final Stopwatch stopwatch = Stopwatch.createStarted();

					final JoinedTupleListFuture result = guimodel.getConnection().querySpatialJoin(
							Arrays.asList(table1, table2), bbox, udfs);
					
					result.waitForCompletion();
					stopwatch.stop();
					
					if(result.isFailed()) {
						logger.error("Got an error" + result.getAllMessages());
						return;
					}
					
					final List<OverlayElementGroup> elements = new ArrayList<>();
					final List<Color> colors = Arrays.asList(color1, color2);
					
					for(final MultiTuple joinedTuple : result) {
						final OverlayElementGroup group 
							= OverlayElementBuilder.createOverlayElementGroup(joinedTuple, colors);
						
						elements.add(group);
					}
					
					logger.info("Got {} tuples back in {} / {} ms", 
							elements.size(), result.getCompletionTime(TimeUnit.MILLISECONDS), stopwatch.elapsed(TimeUnit.MILLISECONDS));
					
					
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
					
					final List<UserDefinedFilterDefinition> udfs = new ArrayList<>();
					
					if(customFilter.length() > 1) {
						final UserDefinedFilterDefinition udf = new UserDefinedFilterDefinition(customFilter, customValue);
						udfs.add(udf);
					}
					
					final Stopwatch stopwatch = Stopwatch.createStarted();
					
					final TupleListFuture result = guimodel.getConnection().queryRectangle(
							table, bbox, udfs);
					
					result.waitForCompletion();
					stopwatch.stop();
					
					if(result.isFailed()) {
						logger.error("Got an error" + result.getAllMessages());
						return;
					}
					
					final List<OverlayElementGroup> elements = new ArrayList<>();
					
					for(final Tuple tuple : result) {
						final OverlayElementGroup overlayElement = OverlayElementBuilder.createOverlayElementGroup(tuple, table, color);
						elements.add(overlayElement);
					}
					
					logger.info("Got {} tuples back in {} / {} ms", 
							elements.size(), result.getCompletionTime(TimeUnit.MILLISECONDS), stopwatch.elapsed(TimeUnit.MILLISECONDS));
					
					painter.addElementToDrawBulk(elements);
					
				} catch (BBoxDBException e) {
					logger.error("Got error while performing query", e);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return;
				}
			}

			/**
			 * Execute a continuous join query
			 * @param customValue 
			 * @param customFilter 
			 * @param table 
			 * @param bbox 
			 * @param receiveInvalidations 
			 * @param receiveWatermarks 
			 */
			private void executeJoinQueryContinuous(final Hyperrectangle bbox, final String table1, 
					final String table2, final Color color1, final Color color2, 
					final String customFilter, final String customValue,
					final boolean receiveWatermarks, final boolean receiveInvalidations) {
							
				QueryPlanBuilder qpb = QueryPlanBuilder
						.createQueryOnTable(table1)
						.spatialJoinWithTable(table2)
						.forAllNewTuplesInSpace(bbox);
				
				if(customFilter.length() > 2) {
					final UserDefinedFilterDefinition userDefinedFilter 
						= new UserDefinedFilterDefinition(customFilter, customValue);
					
					qpb.addJoinFilter(userDefinedFilter);
				}
				
				if(receiveWatermarks) {
					qpb.receiveWatermarks();
				}
				
				if(receiveInvalidations) {
					qpb.receiveInvalidations();
				}
				
				final BBoxDBCluster connection = guimodel.getConnection();
				final List<Color> colors = Arrays.asList(color1, color2);

				final ContinuousQueryPlan qp = qpb.build();
		
				startQueryRunable(qp, connection, colors);
			}

			/**
			 * Start the given query runable
			 * @param qp
			 * @param connection
			 * @param colors
			 */
			private void startQueryRunable(final ContinuousQueryPlan qp, final BBoxDBCluster connection,
					final List<Color> colors) {
				
				final Runnable runable = new ContinuousQueryRunable(colors, qp, connection, painter);
				
				final Thread fetchThread = new Thread(runable);
				backgroundThreads.add(fetchThread);
				fetchThread.start();
			}
			
			/**
			 * Execute a continuous range query
			 * @param customValue 
			 * @param customFilter 
			 * @param table 
			 * @param bbox 
			 * @param receiveInvalidations 
			 * @param receiveWatermarks 
			 */
			private void executeRangeQueryContinuous(final Hyperrectangle bbox, final String table, 
					final Color color, final String customFilter, final String customValue, 
					final boolean receiveWatermarks, final boolean receiveInvalidations) {
							
				final QueryPlanBuilder qpb = QueryPlanBuilder
						.createQueryOnTable(table)
						.forAllNewTuplesInSpace(bbox)
						.compareWithStaticSpace(bbox);
				
				if(customFilter.length() > 2) {
					final UserDefinedFilterDefinition userDefinedFilter 
						= new UserDefinedFilterDefinition(customFilter, customValue);
					
					qpb.addStreamFilter(userDefinedFilter);
				}
				
				if(receiveWatermarks) {
					qpb.receiveWatermarks();
				}
				
				if(receiveInvalidations) {
					qpb.receiveInvalidations();
				}
				
				final BBoxDBCluster connection = guimodel.getConnection();
				
				final ContinuousQueryPlan qp = qpb.build();

				final List<Color> colors = Arrays.asList(color);
				startQueryRunable(qp, connection, colors);
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

