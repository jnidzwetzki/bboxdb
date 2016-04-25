package de.fernunihagen.dna.jkn.scalephant.tools.gui;

import java.awt.Dimension;
import java.awt.Frame;

public class GuiHelper {
	
	/**
     * Set the position of the frame to the center of the screen
     * 
     */
    public static void setCenterPosition(final Frame frame) {
        final Dimension paneSize   = frame.getSize();
        final Dimension screenSize = frame.getToolkit().getScreenSize();
        frame.setLocation(
            (screenSize.width  - paneSize.width)  / 2,
            (screenSize.height - paneSize.height) / 2);
    }
}
