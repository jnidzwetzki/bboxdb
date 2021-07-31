/*******************************************************************************
 *
 *    Copyright (C) 2015-2021 the BBoxDB project
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
package org.bboxdb.test.query;

import java.util.Optional;
import java.util.Set;

import org.bboxdb.network.server.query.continuous.ContinuousQueryExecutionState;
import org.bboxdb.network.server.query.continuous.IdleQueryStateResult;
import org.junit.Assert;
import org.junit.Test;

public class TestContinuousQueryExecutionState {

	@Test(timeout = 60_000)
	public void testStreamState() {
		final ContinuousQueryExecutionState state = new ContinuousQueryExecutionState();
	
		Assert.assertFalse(state.wasStreamKeyContainedInLastRangeQuery("abc"));
		state.addStreamKeyToState("abc");
		Assert.assertTrue(state.wasStreamKeyContainedInLastRangeQuery("abc"));

		Assert.assertTrue(state.removeStreamKeyFromRangeState("abc"));
		Assert.assertFalse(state.removeStreamKeyFromRangeState("abc"));
		Assert.assertFalse(state.wasStreamKeyContainedInLastRangeQuery("abc"));
	}
	
	@Test(timeout = 60_000)
	public void testJoinState() {
		final ContinuousQueryExecutionState state = new ContinuousQueryExecutionState();
	
		Assert.assertFalse(state.wasStreamKeyContainedInLastJoinQuery("stream"));
		
		state.clearJoinPartnerState();
		state.addJoinCandidateForCurrentKey("abc");
		state.addJoinCandidateForCurrentKey("def");
		
		final Set<String> missingPartners1 = state.commitStateAndGetMissingJoinpartners("stream");
		Assert.assertTrue(missingPartners1.isEmpty());
		Assert.assertTrue(state.wasStreamKeyContainedInLastJoinQuery("stream"));

		final Set<String> missingPartners2 = state.commitStateAndGetMissingJoinpartners("stream");
		Assert.assertEquals(2, missingPartners2.size());
		Assert.assertTrue(missingPartners2.contains("abc"));
		Assert.assertTrue(missingPartners2.contains("def"));

		state.clearJoinPartnerState();
		state.addJoinCandidateForCurrentKey("abc");
		state.addJoinCandidateForCurrentKey("def");
		final Set<String> missingPartners3 = state.commitStateAndGetMissingJoinpartners("stream");
		Assert.assertTrue(missingPartners3.isEmpty());
		
		state.addJoinCandidateForCurrentKey("def");
		final Set<String> missingPartners4 = state.commitStateAndGetMissingJoinpartners("stream");
		Assert.assertEquals(1, missingPartners4.size());
		Assert.assertTrue(missingPartners4.contains("abc"));
		
		// Clear
		state.removeStreamKeyFromJoinState("stream");
		Assert.assertFalse(state.wasStreamKeyContainedInLastJoinQuery("stream"));
		final Set<String> missingPartners5 = state.commitStateAndGetMissingJoinpartners("stream");
		Assert.assertTrue(missingPartners5.isEmpty());

	}

	@Test(timeout = 60_000)
	public void testInvalidation() {
		final ContinuousQueryExecutionState state = new ContinuousQueryExecutionState();
		
		state.setCurrentWatermarkGeneration(1);
		state.addStreamKeyToState("abc");

		Assert.assertTrue(state.wasStreamKeyContainedInLastRangeQuery("abc"));
		final Optional<IdleQueryStateResult> result1 = state.invalidateIdleEntries(2, 0);
		Assert.assertFalse(result1.isPresent());
		Assert.assertTrue(state.wasStreamKeyContainedInLastRangeQuery("abc"));

		final Optional<IdleQueryStateResult> result2 = state.invalidateIdleEntries(2, 1);
		Assert.assertTrue(result2.isPresent());
		Assert.assertFalse(state.wasStreamKeyContainedInLastRangeQuery("abc"));
		Assert.assertTrue(result2.get().getRemovedStreamKeys().contains("abc"));

		state.setCurrentWatermarkGeneration(2);
		state.addStreamKeyToState("abc");
		final Optional<IdleQueryStateResult> result3 = state.invalidateIdleEntries(3, 2);
		Assert.assertTrue(result3.isPresent());
		Assert.assertTrue(result3.get().getRemovedStreamKeys().isEmpty());
		Assert.assertTrue(state.wasStreamKeyContainedInLastRangeQuery("abc"));
		
		final Optional<IdleQueryStateResult> result4 = state.invalidateIdleEntries(4, 2);
		Assert.assertTrue(result4.isPresent());
		Assert.assertFalse(state.wasStreamKeyContainedInLastRangeQuery("abc"));
		Assert.assertTrue(result4.get().getRemovedStreamKeys().contains("abc"));
	}
}
