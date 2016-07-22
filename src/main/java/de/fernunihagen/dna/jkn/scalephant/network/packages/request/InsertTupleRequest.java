package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkTupleEncoderDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.routing.RoutingHeader;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.SSTableName;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.TupleAndTable;

public class InsertTupleRequest implements NetworkRequestPackage {

	/**
	 * The name of the table
	 */
	protected final SSTableName table;
	
	/**
	 * The Tuple
	 */
	protected final Tuple tuple;
	
	/**
	 * A routing header for custom routing
	 */
	protected RoutingHeader routingHeader;
	
	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(InsertTupleRequest.class);
	
	
	/**
	 * Create package from parameter
	 * 
	 * @param table
	 * @param key
	 * @param timestamp
	 * @param bbox
	 * @param data
	 */
	public InsertTupleRequest(final RoutingHeader routingHeader, final SSTableName table, final Tuple tuple) {
		this.routingHeader = routingHeader;
		this.table = table;
		this.tuple = tuple;
	}
	
	public InsertTupleRequest(final SSTableName table, final Tuple tuple) { 
		this(new RoutingHeader(false), table, tuple);
	}
	
	/**
	 * Decode the encoded tuple into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws IOException 
	 */
	public static InsertTupleRequest decodeTuple(final ByteBuffer encodedPackage) throws IOException {

		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);

		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		final RoutingHeader routingHeader = NetworkPackageDecoder.getRoutingHeaderFromRequestPackage(encodedPackage);
		
		final SSTableName ssTableName = new SSTableName(tupleAndTable.getTable());
		return new InsertTupleRequest(routingHeader, ssTableName, tupleAndTable.getTuple());
	}

	@Override
	public void writeToOutputStream(final short sequenceNumber, final OutputStream outputStream) {

		try {
			final byte[] tupleAsByte = NetworkTupleEncoderDecoder.encode(tuple, table.getFullname());
			
			// Body length
			final long bodyLength = tupleAsByte.length;
			
			// Unrouted package
			NetworkPackageEncoder.appendRequestPackageHeader(sequenceNumber, bodyLength, routingHeader, 
					getPackageType(), outputStream);

			// Write tuple
			outputStream.write(tupleAsByte);
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
		}		
	}
	
	/**
	 * Get the referenced table
	 * @return
	 */
	public SSTableName getTable() {
		return table;
	}

	/**
	 * Get the referenced tuple
	 * @return
	 */
	public Tuple getTuple() {
		return tuple;
	}

	/**
	 * Get the routing header
	 * @return
	 */
	public RoutingHeader getRoutingHeader() {
		return routingHeader;
	}
	
	/**
	 * Replace the existing routing header with a new one
	 * @param routingHeader
	 */
	public void replaceRoutingHeader(final RoutingHeader routingHeader) {
		this.routingHeader = routingHeader;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + ((tuple == null) ? 0 : tuple.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InsertTupleRequest other = (InsertTupleRequest) obj;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (tuple == null) {
			if (other.tuple != null)
				return false;
		} else if (!tuple.equals(other.tuple))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "InsertTupleRequest [table=" + table + ", tuple=" + tuple + ", routingHeader=" + routingHeader + "]";
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_INSERT_TUPLE;
	}

}
