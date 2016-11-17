package de.fernunihagen.dna.scalephant.performance.osm.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import de.fernunihagen.dna.scalephant.storage.entity.BoundingBox;

public class GeometricalStructure implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2119998223400835002L;
	
	/**
	 * The ID of the sttructure
	 */
	protected final long id;
	
	/**
	 * The list of our points
	 */
	protected List<OSMPoint> pointList = new ArrayList<OSMPoint>();
	
	public GeometricalStructure(final long id) {
		super();
		this.id = id;
	}

	public void addPoint(final double d, final double e) {
		final OSMPoint point = new OSMPoint(d, e);
		pointList.add(point);
	}
	
	/**
	 * Get the number of points
	 * @return 
	 */
	public int getNumberOfPoints() {
		return pointList.size();
	}
	
	/**
	 * Get the bounding box from the object
	 * @return
	 */
	public BoundingBox getBoundingBox() {
		
		if(pointList.isEmpty()) {
			return BoundingBox.EMPTY_BOX;
		}
		
		final OSMPoint firstPoint = pointList.get(0);
		double minX = firstPoint.getX();
		double maxX = firstPoint.getX();
		double minY = firstPoint.getY();
		double maxY = firstPoint.getY();
		
		for(final OSMPoint osmPoint : pointList) {
			minX = Math.min(minX, osmPoint.getX());
			maxX = Math.min(maxX, osmPoint.getX());
			minY = Math.min(minY, osmPoint.getY());
			maxY = Math.min(maxY, osmPoint.getY());
		}
		
		return new BoundingBox((float) minX, (float) maxX, (float) minY, (float) maxY);
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

	public long getId() {
		return id;
	}
	
}
