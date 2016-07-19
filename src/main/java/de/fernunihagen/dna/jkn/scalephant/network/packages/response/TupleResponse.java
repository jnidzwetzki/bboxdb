package de.fernunihagen.dna.jkn.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.network.NetworkConst;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.jkn.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.jkn.scalephant.network.packages.NetworkTupleEncoderDecoder;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.jkn.scalephant.storage.entity.TupleAndTable;
import de.fernunihagen.dna.jkn.scalephant.tools.DataEncoderHelper;

public class TupleResponse extends NetworkResponsePackage {
	
	/**
	 * The table
	 */
	protected final String table;
	
	/**
	 * The tuple
	 */
	protected final Tuple tuple;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(TupleResponse.class);

	public TupleResponse(final short sequenceNumber, final String table, final Tuple tuple) {
		super(sequenceNumber);
		this.table = table;
		this.tuple = tuple;
	}
	
	@Override
	public byte getPackageType() {
			return NetworkConst.RESPONSE_TUPLE;
	}

	@Override
	public byte[] getByteArray() {
		
		final NetworkPackageEncoder networkPackageEncoder = new NetworkPackageEncoder();
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());

			try {
				final byte[] encodedBytes = NetworkTupleEncoderDecoder.encode(tuple, table);
				final ByteBuffer lengthBytes = DataEncoderHelper.longToByteBuffer(encodedBytes.length);
				bos.write(lengthBytes.array());
				bos.write(encodedBytes);
				bos.close();
			} catch (IOException e) {
				logger.error("Got exception while converting package into bytes", e);
				return null;
			}
	
		return bos.toByteArray();
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 */
	public static TupleResponse decodePackage(final ByteBuffer encodedPackage) {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TUPLE);

		if(decodeResult == false) {
			logger.warn("Unable to decode package");
			return null;
		}
		
		final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);
		
		if(encodedPackage.remaining() != 0) {
			logger.error("Some bytes are left after encoding: " + encodedPackage.remaining());
		}
		
		return new TupleResponse(requestId, tupleAndTable.getTable(), tupleAndTable.getTuple());
	}

	public String getTable() {
		return table;
	}

	public Tuple getTuple() {
		return tuple;
	}
}
