package de.fernunihagen.dna.jkn.scalephant.network.client;

import java.util.concurrent.atomic.AtomicInteger;

public class SequenceNumberGenerator {

	/**
	 * The next unused free request id
	 */
	protected final AtomicInteger sequenceNumber = new AtomicInteger();
	
	public SequenceNumberGenerator() {
		sequenceNumber.set(0);
	}
	
	/**
	 * Get the next free sequence number
	 * 
	 * @return The sequence number
	 */
	public short getNextSequenceNummber() {
		return (short) sequenceNumber.getAndIncrement();
	}
	
	/**
	 * Get the curent sequence number
	 * 
	 * @return The sequence number
	 */
	public short getSequeneNumberWithoutIncrement() {
		return sequenceNumber.shortValue();
	}
}
