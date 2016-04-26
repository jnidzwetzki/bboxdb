package de.fernunihagen.dna.jkn.scalephant.tools.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Point;
import java.awt.RenderingHints;
import java.awt.event.ActionEvent;
import java.text.SimpleDateFormat;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScalephantGUI {
	
	protected JFrame mainframe;
	protected JPanel cassandraPanel;
	protected JMenuBar menuBar;
	protected ScalepahntInstanceTableModel tableModel;
	protected GUIModel guiModel;
	protected int totalTokenRanges;
	
	protected final static Logger logger = LoggerFactory.getLogger(ScalephantGUI.class);

	public final static int SIZE = 400;
	public final Point upperPoint = new Point(200, 30);
	public final Point centerPoint = new Point(upperPoint.x + SIZE/2,  upperPoint.y + SIZE/2);
	public final SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public volatile boolean shutdown = false;
	
	public ScalephantGUI(final GUIModel guiModel) {
		this.guiModel = guiModel;
	}

	/**
	 * Build the DSECONDO dialog, init GUI components
	 * and assemble the dialog
	 */
	public void run() {
		
		mainframe = new JFrame("Scalephant - Data Distribution");
		
		setupMenu();
		setupCassandraPanel();
		
		tableModel = getTableModel();
		final JTable table = new JTable(tableModel);
		table.getColumnModel().getColumn(0).setMaxWidth(40);
		table.getColumnModel().getColumn(2).setMinWidth(100);
		table.getColumnModel().getColumn(2).setMaxWidth(100);

		final JScrollPane scrollPane = new JScrollPane(table);		
		final Dimension d = table.getPreferredSize();
		
		scrollPane.setPreferredSize(
		    new Dimension(d.width,table.getRowHeight()*7));
		
		mainframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainframe.setLayout(new BorderLayout());
		mainframe.add(cassandraPanel, BorderLayout.CENTER);
		mainframe.add(scrollPane, BorderLayout.SOUTH);

		mainframe.pack();
		GuiHelper.setCenterPosition(mainframe);
		mainframe.setVisible(true);
	}

	/** 
	 * Dispose the main frame
	 */
	public void dispose() {
		mainframe.dispose();
	}
	
	/**
	 * Get the table model for the schedules queries
	 * @return The table model
	 */
	private ScalepahntInstanceTableModel getTableModel() {
		final List<String> scalepahntInstances = guiModel.getScalephantInstances();
		return new ScalepahntInstanceTableModel(scalepahntInstances);
	}

	/**
	 * Initalize the GUI panel
	 * 
	 */
	protected void setupCassandraPanel() {
		cassandraPanel = new JPanel() {
		
			private static final long serialVersionUID = -248493308846818192L;

			@Override
			protected void paintComponent(Graphics g) {
			
				super.paintComponent(g);
				
	            Graphics2D graphics2D = (Graphics2D)g;
	            
	            graphics2D.setRenderingHint(
	                    RenderingHints.KEY_ANTIALIASING, 
	                    RenderingHints.VALUE_ANTIALIAS_ON);
			}
			
		};
		
		cassandraPanel.setToolTipText("");
		
		cassandraPanel.setPreferredSize(new Dimension(800, 500));
	}

	/**
	 * Create the menu of the main window
	 */
	protected void setupMenu() {
		menuBar = new JMenuBar();
		JMenu menu = new JMenu("File");
		menuBar.add(menu);
		
		JMenuItem menuItem = new JMenuItem("Close");
		menuItem.addActionListener(new AbstractAction() {
			
			private static final long serialVersionUID = -5380326547117916348L;

			public void actionPerformed(ActionEvent e) {
				shutdown = true;
			}
			
		});
		menu.add(menuItem);
        mainframe.setJMenuBar(menuBar);
	}
	
	/**
	 * Update the gui model
	 */
	public synchronized void updateStatus() {
		long curTime = System.currentTimeMillis();

		
	}
	
	/**
	 * Update the view. This method should be called periodically
	 */
	public void updateView() {
		updateStatus();
		mainframe.repaint();
	}
	
}