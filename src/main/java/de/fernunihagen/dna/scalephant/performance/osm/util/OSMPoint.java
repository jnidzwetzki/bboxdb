package de.fernunihagen.dna.scalephant.performance.osm.util;

import java.io.Serializable;

public class OSMPoint implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7960562080700743356L;

	/**
	 * The x value
	 */
	protected double x;
	
	/**
	 * The y value
	 */
	protected double y;

	public OSMPoint(final double x, final double y) {
		super();
		this.x = x;
		this.y = y;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}

	@Override
	public String toString() {
		return "OSMPoint [x=" + x + ", y=" + y + "]";
	}
	
}
