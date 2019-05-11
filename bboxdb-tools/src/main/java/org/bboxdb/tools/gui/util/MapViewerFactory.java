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
package org.bboxdb.tools.gui.util;

import javax.swing.JButton;
import javax.swing.event.MouseInputListener;

import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.OSMTileFactoryInfo;
import org.jxmapviewer.input.PanMouseInputListener;
import org.jxmapviewer.viewer.DefaultTileFactory;
import org.jxmapviewer.viewer.GeoPosition;
import org.jxmapviewer.viewer.TileFactoryInfo;

public class MapViewerFactory {

	public static JXMapViewer createMapViewer() {
		final JXMapViewer mapViewer = new JXMapViewer();

		// Create a TileFactoryInfo for OpenStreetMap
		final TileFactoryInfo info = new OSMTileFactoryInfo();
		final DefaultTileFactory tileFactory = new DefaultTileFactory(info);
		mapViewer.setTileFactory(tileFactory);
	
		final MouseInputListener mia = new PanMouseInputListener(mapViewer);
		mapViewer.addMouseListener(mia);
		mapViewer.addMouseMotionListener(mia);
		
		// Use 8 threads in parallel to load the tiles
		tileFactory.setThreadPoolSize(8);
		
		// Show start point
		showHagen(mapViewer);
		
		return mapViewer;
	}
	
	/**
	 * Show the university of Hagen in the viewer
	 * @param mapViewer
	 */
	public static void showHagen(final JXMapViewer mapViewer) {
		final GeoPosition hagen = new GeoPosition(51.376255, 7.493675);
		mapViewer.setZoom(7);
		mapViewer.setAddressLocation(hagen);
	}

	/**
	 * Get the show world button
	 * @return
	 */
	public static JButton getShowWorldButton(final JXMapViewer mapViewer) {
		final JButton showWorldButton = new JButton("Show world");
		showWorldButton.addActionListener((l) -> {
			mapViewer.setZoom(17);
		}
		); 
		return showWorldButton;
	}

	/**
	 * Get the zoom in button
	 * @return
	 */
	public static JButton getZoomInButton(final JXMapViewer mapViewer) {
		final JButton zoomInButton = new JButton("Zoom in");
		zoomInButton.addActionListener((l) -> {
			final int zoom = mapViewer.getZoom();
			mapViewer.setZoom(zoom - 1);
		}
		); 
		return zoomInButton;
	}

	/**
	 * Get the zoom out button
	 * @return
	 */
	public static JButton getZoomOutButton(final JXMapViewer mapViewer) {
		final JButton zoomOutButton = new JButton("Zoom out");
		zoomOutButton.addActionListener((l) -> {
			final int zoom = mapViewer.getZoom();
			mapViewer.setZoom(zoom + 1);
		}
		); 
		return zoomOutButton;
	}
	
	/**
	 * Get the show hagen button
	 * @return
	 */
	public static JButton getShowHagenButton(final JXMapViewer mapViewer) {
		final JButton showHagenButton = new JButton("Show Hagen");
		showHagenButton.addActionListener((l) -> {
			showHagen(mapViewer);
		}
		); 
		return showHagenButton;
	}

	
}
