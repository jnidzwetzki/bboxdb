package de.fernunihagen.dna.jkn.scalephant.network.packages.request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.TupleAndTable;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkRequestPackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkTupleEncoderDecoder;
import de.fernunihagen.dna.jkn.scalephant.storage.Tuple;

public class InsertTupleRequest implements NetworkRequestPackage {

	/**
	 * The name of the table
	 */
	protected final String table;
	
	/**
	 * The Tuple
	 */
	protected Tuple tuple;
	
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
	public InsertTupleRequest(final String table, final Tuple tuple) {
		this.table = table;
		this.tuple = tuple;
	}
	
	/**
	 * Decode the encoded tuple into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static InsertTupleRequest decodeTuple(final ByteBuffer encodedPackage) {

		final boolean decodeResult = NetworkPackageDecoder.validateRequestPackageHeader(encodedPackage, NetworkConst.REQUEST_TYPE_INSERT_TUPLE);
		
		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}

		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);
		
		return new InsertTupleRequest(tupleAndTable.getTable(), tupleAndTable.getTuple());
	}

	@Override
	public byte[] getByteArray(final short sequenceNumber) {
		
		final NetworkPackageEncoder networkPackageEncoder 
			= new NetworkPackageEncoder();
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForRequestPackage(sequenceNumber, getPackageType());
		
		try {
			NetworkTupleEncoderDecoder.encode(bos, tuple, table);
			bos.close();
		} catch (IOException e) {
			logger.error("Got exception while converting package into bytes", e);
			return null;
		}
		
		return bos.toByteArray();
	}
	
	public String getTable() {
		return table;
	}

	public Tuple getTuple() {
		return tuple;
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
		return "InsertTupleRequest [table=" + table + ", tuple=" + tuple + "]";
	}

	@Override
	public byte getPackageType() {
		return NetworkConst.REQUEST_TYPE_INSERT_TUPLE;
	}
}
