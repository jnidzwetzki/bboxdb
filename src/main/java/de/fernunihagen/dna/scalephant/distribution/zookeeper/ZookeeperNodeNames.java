package de.fernunihagen.dna.scalephant.distribution.zookeeper;

public class ZookeeperNodeNames {
	/**
	 * The prefix for nodes in sequential queues
	 */
	public final static String SEQUENCE_QUEUE_PREFIX = "id-";
	
	/**
	 * Name of the left tree node
	 */
	public final static String NAME_LEFT = "left";
	
	/**
	 * Name of the right tree node
	 */
	public final static String NAME_RIGHT = "right";
	
	/**
	 * Name of the split node
	 */
	public final static String NAME_SPLIT = "split";
	
	/**
	 * Name of the name prefix node
	 */
	public final static String NAME_NAMEPREFIX = "nameprefix";
	
	/**
	 * Name of the name prefix queue
	 */
	public static final String NAME_PREFIXQUEUE = "nameprefixqueue";

	/**
	 * Name of the replication node
	 */
	public final static String NAME_REPLICATION = "replication";
	
	/**
	 * Name of the systems node
	 */
	public final static String NAME_SYSTEMS = "systems";
	
	/**
	 * Name of the version node
	 */
	public final static String NAME_VERSION = "version";
	
	/**
	 * Name of the state node
	 */
	public final static String NAME_STATE = "state";
}