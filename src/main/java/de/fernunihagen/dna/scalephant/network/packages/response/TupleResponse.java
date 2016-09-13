package de.fernunihagen.dna.scalephant.network.packages.response;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import de.fernunihagen.dna.scalephant.network.NetworkConst;
import de.fernunihagen.dna.scalephant.network.NetworkPackageDecoder;
import de.fernunihagen.dna.scalephant.network.NetworkPackageEncoder;
import de.fernunihagen.dna.scalephant.network.packages.NetworkResponsePackage;
import de.fernunihagen.dna.scalephant.network.packages.NetworkTupleEncoderDecoder;
import de.fernunihagen.dna.scalephant.network.packages.PackageEncodeError;
import de.fernunihagen.dna.scalephant.storage.entity.Tuple;
import de.fernunihagen.dna.scalephant.storage.entity.TupleAndTable;
import de.fernunihagen.dna.scalephant.tools.DataEncoderHelper;

public class TupleResponse extends NetworkResponsePackage {
	
	/**
	 * The table
	 */
	protected final String table;
	
	/**
	 * The tuple
	 */
	protected final Tuple tuple;

	public TupleResponse(final short sequenceNumber, final String table, final Tuple tuple) {
		super(sequenceNumber);
		this.table = table;
		this.tuple = tuple;
	}
	
	@Override
	public byte getPackageType() {
			return NetworkConst.RESPONSE_TYPE_TUPLE;
	}

	@Override
	public byte[] getByteArray() throws PackageEncodeError {
		
		final NetworkPackageEncoder networkPackageEncoder = new NetworkPackageEncoder();
		
		final ByteArrayOutputStream bos = networkPackageEncoder.getOutputStreamForResponsePackage(sequenceNumber, getPackageType());

			try {
				final byte[] encodedBytes = NetworkTupleEncoderDecoder.encode(tuple, table);
				final ByteBuffer lengthBytes = DataEncoderHelper.longToByteBuffer(encodedBytes.length);
				bos.write(lengthBytes.array());
				bos.write(encodedBytes);
				bos.close();
			} catch (IOException e) {
				throw new PackageEncodeError("Got exception while converting package into bytes", e);
			}
	
		return bos.toByteArray();
	}
	
	/**
	 * Decode the encoded package into a object
	 * 
	 * @param encodedPackage
	 * @return
	 * @throws PackageEncodeError 
	 */
	public static TupleResponse decodePackage(final ByteBuffer encodedPackage) throws PackageEncodeError {		
		final short requestId = NetworkPackageDecoder.getRequestIDFromResponsePackage(encodedPackage);

		final boolean decodeResult = NetworkPackageDecoder.validateResponsePackageHeader(encodedPackage, NetworkConst.RESPONSE_TYPE_TUPLE);

		if(decodeResult == false) {
			throw new PackageEncodeError("Unable to decode package");
		}
		
		final TupleAndTable tupleAndTable = NetworkTupleEncoderDecoder.decode(encodedPackage);
		
		if(encodedPackage.remaining() != 0) {
			throw new PackageEncodeError("Some bytes are left after encoding: " + encodedPackage.remaining());
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
