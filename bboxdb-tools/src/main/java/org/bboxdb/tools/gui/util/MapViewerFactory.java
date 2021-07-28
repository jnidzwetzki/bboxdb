/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.swing.JButton;
import javax.swing.event.MouseInputListener;

import org.jxmapviewer.JXMapViewer;
import org.jxmapviewer.OSMTileFactoryInfo;
import org.jxmapviewer.cache.FileBasedLocalCache;
import org.jxmapviewer.input.PanKeyListener;
import org.jxmapviewer.input.PanMouseInputListener;
import org.jxmapviewer.input.ZoomMouseWheelListenerCursor;
import org.jxmapviewer.viewer.DefaultTileFactory;
import org.jxmapviewer.viewer.GeoPosition;
import org.jxmapviewer.viewer.TileFactoryInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapViewerFactory {
	
	/**
	 * The cache dir
	 */
	private static Path cacheDir;
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(MapViewerFactory.class);
	
	static {
		final String tempdir = System.getProperty("java.io.tmpdir");
		final String username = System.getProperty("user.name");

		cacheDir = Paths.get(tempdir, "jxmapviewer2cache_" + username);
		final File file = cacheDir.toFile();
		
		final boolean createSuccess = file.mkdirs();
		
		if(! createSuccess) {
			logger.debug("Using existing cache dir");
		}
		
		if(! file.canWrite()) {
			logger.error("Unable to write to cache dir: " + file);
			System.exit(-1);
		}

		logger.info("Caching maps to: " + cacheDir);
	}

	public static JXMapViewer createMapViewer() {
		final JXMapViewer mapViewer = new JXMapViewer();

		// Create a TileFactoryInfo for OpenStreetMap
		final TileFactoryInfo info = new OSMTileFactoryInfo();
		final DefaultTileFactory tileFactory = new DefaultTileFactory(info);
        tileFactory.setLocalCache(new FileBasedLocalCache(cacheDir.toFile(), false));
		mapViewer.setTileFactory(tileFactory);
	
		final MouseInputListener mia = new PanMouseInputListener(mapViewer);
		mapViewer.addMouseListener(mia);
		mapViewer.addMouseMotionListener(mia);
		
        mapViewer.addMouseWheelListener(new ZoomMouseWheelListenerCursor(mapViewer));
		
        mapViewer.addKeyListener(new PanKeyListener(mapViewer));
		
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
	 * Show LA in the viewer
	 * @param mapViewer
	 */
	public static void showLA(final JXMapViewer mapViewer) {
		final GeoPosition la = new GeoPosition(34.052235, -118.243683);
		mapViewer.setZoom(9);
		mapViewer.setAddressLocation(la);
	}
	
	/**
	 * Show the sydney in the viewer
	 * @param mapViewer
	 */
	public static void showSydney(final JXMapViewer mapViewer) {
		final GeoPosition sydney = new GeoPosition(-33.865143, 151.2099);
		mapViewer.setZoom(9);
		mapViewer.setAddressLocation(sydney);
	}

	/**
	 * Show Berlin in the viewer
	 * @param mapViewer
	 */
	public static void showBerlin(final JXMapViewer mapViewer) {
		final GeoPosition hagen = new GeoPosition(52.522199, 13.413749);
		mapViewer.setZoom(9);
		mapViewer.setAddressLocation(hagen);
	}
	
	/**
	 * Show Germany in the viewer
	 * @param mapViewer
	 */
	public static void showGermany(final JXMapViewer mapViewer) {
		final GeoPosition hagen = new GeoPosition(51.904815, 10.265166);
		mapViewer.setZoom(14);
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
	
	/**
	 * Get the show hagen button
	 * @return
	 */
	public static JButton getShowLAButton(final JXMapViewer mapViewer) {
		final JButton showLAButton = new JButton("Show LA");
		showLAButton.addActionListener((l) -> {
			showLA(mapViewer);
		}
		); 
		return showLAButton;
	}
	
	/**
	 * Get the show hagen button
	 * @return
	 */
	public static JButton getShowSydneyButton(final JXMapViewer mapViewer) {
		final JButton showHagenButton = new JButton("Show Sydney");
		showHagenButton.addActionListener((l) -> {
			showSydney(mapViewer);
		}
		); 
		return showHagenButton;
	}
	
	/**
	 * Get the show germany button
	 * @return
	 */
	public static JButton getShowGermanyButton(final JXMapViewer mapViewer) {
		final JButton showGermanyButton = new JButton("Show Germany");
		showGermanyButton.addActionListener((l) -> {
			showGermany(mapViewer);
		}
		); 
		return showGermanyButton;
	}
	
	/**
	 * Get the show hagen button
	 * @return
	 */
	public static JButton getShowBerlinButton(final JXMapViewer mapViewer) {
		final JButton showHagenButton = new JButton("Show Berlin");
		showHagenButton.addActionListener((l) -> {
			showBerlin(mapViewer);
		}
		); 
		return showHagenButton;
	}


	
}
