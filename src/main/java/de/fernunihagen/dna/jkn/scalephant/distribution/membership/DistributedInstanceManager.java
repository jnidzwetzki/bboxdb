package de.fernunihagen.dna.jkn.scalephant.distribution.membership;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceAddEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceDeleteEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEvent;
import de.fernunihagen.dna.jkn.scalephant.distribution.membership.event.DistributedInstanceEventCallback;

public class DistributedInstanceManager {

	/**
	 * The event listener
	 */
	protected final List<DistributedInstanceEventCallback> listener = new CopyOnWriteArrayList<DistributedInstanceEventCallback>();
	
	/**
	 * The active scalephant instances
	 */
	protected final Set<DistributedInstance> instances = new HashSet<DistributedInstance>();
	
	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(DistributedInstanceManager.class);

	/**
	 * The instance
	 */
	protected static DistributedInstanceManager instance;
	
	/**
	 * Get the instance
	 * @return
	 */
	public static synchronized DistributedInstanceManager getInstance() {
		if(instance == null) {
			instance = new DistributedInstanceManager();
		}
		
		return instance;
	}
	
	/**
	 * Singletons are not clonable
	 */
	@Override
	protected Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException("Unable to clone a singleton");
	}
	
	/**
	 * Update the instance list, called from zookeeper client
	 * @param newInstances
	 */
	public void updateInstanceList(final Set<DistributedInstance> newInstances) {
		
		// Check the membership of the old members
		for(final DistributedInstance instance : instances) {
			if(! newInstances.contains(instance) ) {
				instances.remove(instance);
				sendEvent(new DistributedInstanceDeleteEvent(instance));
			}
		}
		
		// Any new member?
		for(final DistributedInstance instance : newInstances) {
			if( ! instances.contains(instance)) {
				instances.add(instance);
				sendEvent(new DistributedInstanceAddEvent(instance));
			}
		}
	}
	
	/**
	 * Send a event to all the listeners
	 * 
	 * @param event
	 */
	protected void sendEvent(final DistributedInstanceEvent event) {
		logger.info("Sending event: " + event);

		for(final DistributedInstanceEventCallback callback : listener) {
			callback.distributedInstanceEvent(event);
		}
	}
	
	/**
	 * Register a callback listener
	 * @param callback
	 */
	public void registerListener(final DistributedInstanceEventCallback callback) {
		listener.add(callback);
	}
	
	/**
	 * Remove a callback listener
	 * @param callback
	 */
	public void removeListener(final DistributedInstanceEventCallback callback) {
		listener.remove(callback);
	}
}
