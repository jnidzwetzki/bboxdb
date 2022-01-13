/*******************************************************************************
 *
 *    Copyright (C) 2015-2022 the BBoxDB project
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 *******************************************************************************/
package org.bboxdb.network.server.query.continuous;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousQueryExecutionState {

	/**
	 * The stream tuple that are matched in the last query execution
	 */
	protected final Set<String> containedStreamKeys;
	
	/**
	 * The stream tuples and their join partners that are used in the last execution
	 */
	protected final Map<String, Set<String>> containedJoinedKeys;
	
	/**
	 * The names of the join partners for the current key
	 */
	protected final Set<String> joinPartnersForCurrentKey;
	
	/**
	 * The watermark idle state
	 */
	protected final Map<String, Long> watermarkIdleState;
	
	/**
	 * The current watermark generation
	 */
	private long currentWatermarkGeneration;

	/**
	 * The Logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(ContinuousQueryExecutionState.class);
	
	public ContinuousQueryExecutionState() {
		this.containedStreamKeys = new HashSet<>();
		this.containedJoinedKeys = new HashMap<>();
		this.joinPartnersForCurrentKey = new HashSet<>();
		this.watermarkIdleState = new HashMap<>();
		this.currentWatermarkGeneration = 0;
	}
	
	/**
	 * Was the stream key contained in the last range query
	 * @param key
	 * @return
	 */
	public boolean wasStreamKeyContainedInLastRangeQuery(final String key) {
		return containedStreamKeys.contains(key);
	}
	
	/**
	 * Remove the given key from the state
	 * @param key
	 * @return
	 */
	public boolean removeStreamKeyFromRangeState(final String key) {
		watermarkIdleState.remove(key);
		return containedStreamKeys.remove(key);
	}
	
	/**
	 * Add the given key to the state
	 * @param key
	 */
	public void addStreamKeyToState(final String key) {
		watermarkIdleState.put(key, currentWatermarkGeneration);
		containedStreamKeys.add(key);
	}
	
	/**
	 * Add the current key to the list of the join partners
	 * @param key
	 */
	public void addJoinCandidateForCurrentKey(final String key) {
		watermarkIdleState.put(key, currentWatermarkGeneration);
		joinPartnersForCurrentKey.add(key);
	}
	
	/**
	 * Get the missing join partners for the current key and clear the state
	 * @param streamKey
	 * @return 
	 */
	public Set<String> commitStateAndGetMissingJoinpartners(final String streamKey) {
		
		final Set<String> oldJoinPartners = containedJoinedKeys.getOrDefault(streamKey, new HashSet<>());
		
		// Calculate the difference between the current join partners and the previous join partners
		oldJoinPartners.removeAll(joinPartnersForCurrentKey);
		
		final Set<String> seenJoinPartners = new HashSet<>(joinPartnersForCurrentKey);
		containedJoinedKeys.put(streamKey, seenJoinPartners);
		
		clearJoinPartnerState();
				
		return oldJoinPartners;
	}

	/**
	 * Clear the state for the current join partner
	 */
	public void clearJoinPartnerState() {
		joinPartnersForCurrentKey.clear();
	}
	
	/**
	 * Get the contained joined keys
	 * @return
	 */
	public Map<String, Set<String>> getContainedJoinedKeys() {
		return containedJoinedKeys;
	}
	
	/**
	 * Remove the stream key from join state
	 * @param streamKey
	 * @return 
	 */
	public Set<String> removeStreamKeyFromJoinState(final String streamKey) {
		return containedJoinedKeys.remove(streamKey);
	}
	
	/**
	 * Was the stream key contained in the last join query
	 * @param key
	 * @return
	 */
	public boolean wasStreamKeyContainedInLastJoinQuery(final String key) {
		return containedJoinedKeys.containsKey(key);
	}
	
	/**
	 * Get the contained range query keys
	 * @return
	 */
	public Set<String> getContainedTupleKeys() {
		return containedStreamKeys;
	}

	/**
	 * Merge the given state into the local one
	 * @param resultState
	 */
	public void merge(final Set<String> rangeQueryState, final Map<String, Set<String>> joinQueryState) {
		containedStreamKeys.addAll(rangeQueryState);
		containedJoinedKeys.putAll(joinQueryState);
	}

	/**
	 * Invalidate entries based on the watermark generation
	 * 
	 * @param watermarkGeneration
	 * @param invalidationGenerations
	 * @return 
	 */
	public Optional<IdleQueryStateResult> invalidateIdleEntries(final long watermarkGeneration, final long invalidationGenerations) {
		logger.debug("Invalidating old entries current watermark generation {} / old generations {}", 
				watermarkGeneration, invalidationGenerations);
		
		if(invalidationGenerations == 0) {
			return Optional.empty();
		}
		
        final List<Entry<String, Long>> elementsToRemove = watermarkIdleState
                .entrySet()
                .stream()
                .filter(e -> (e.getValue() <= watermarkGeneration - invalidationGenerations))
                .collect(Collectors.toList());
		
        logger.debug("Removing {} idle entries from state");
		
        final Set<String> removedStreamKeys = new HashSet<>();
        final Map<String, Set<String>> removedJoinPartners = new HashMap<>();
        
		for(final Entry<String, Long> entry : elementsToRemove) {
			final String key = entry.getKey();
			watermarkIdleState.remove(key);
			containedStreamKeys.remove(key);
			
			final Set<String> removedJoinPartnersForKey = containedJoinedKeys.remove(key);
			
			removedStreamKeys.add(key);
			removedJoinPartners.put(key, removedJoinPartnersForKey);
		}
		
        final IdleQueryStateResult idleQueryStateResult = new IdleQueryStateResult(removedStreamKeys, removedJoinPartners);

        return Optional.of(idleQueryStateResult);
		
	}
	
	/**
	 * Get the current watermark generation
	 * @return
	 */
	public long getCurrentWatermarkGeneration() {
		return currentWatermarkGeneration;
	}
	
	/**
	 * Set the current watermark generation
	 * @param currentWatermarkGeneration
	 */
	public void setCurrentWatermarkGeneration(final long currentWatermarkGeneration) {
		this.currentWatermarkGeneration = currentWatermarkGeneration;
	}
}
