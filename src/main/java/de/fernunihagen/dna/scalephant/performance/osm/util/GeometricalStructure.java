package de.fernunihagen.dna.scalephant.performance.osm.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class GeometricalStructure implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2119998223400835002L;
	
	/**
	 * The list of our points
	 */
	protected List<OSMPoint> pointList = new ArrayList<OSMPoint>();
	
	public void addPoint(final double d, final double e) {
		final OSMPoint point = new OSMPoint(d, e);
		pointList.add(point);
	}

	/**
	 * Convert object to byte array
	 * @return
	 * @throws IOException 
	 */
	public byte[] toByteArray() throws IOException {
		final ByteArrayOutputStream bOutputStream = new ByteArrayOutputStream();
		final ObjectOutputStream objectOutputStream = new ObjectOutputStream(bOutputStream);

		objectOutputStream.writeObject(this);
		
		bOutputStream.close();
		return bOutputStream.toByteArray();
	}
	
	/**
	 * Construct object from byte array
	 * @param bytes
	 * @return
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	public GeometricalStructure loadFromByteArray(final byte[] bytes) throws IOException, ClassNotFoundException {
		final ByteArrayInputStream bInputStream = new ByteArrayInputStream(bytes);
		final ObjectInputStream oInputStream = new ObjectInputStream(bInputStream);
		return (GeometricalStructure) oInputStream.readObject();
	}
	
}
