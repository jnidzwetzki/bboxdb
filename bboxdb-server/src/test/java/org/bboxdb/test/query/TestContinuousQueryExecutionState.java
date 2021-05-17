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

import java.util.Set;

import org.bboxdb.network.server.query.continuous.ContinuousQueryExecutionState;
import org.junit.Assert;
import org.junit.Test;

public class TestContinuousQueryExecutionState {

	@Test(timeout = 60_000)
	public void testStreamState() {
		final ContinuousQueryExecutionState state = new ContinuousQueryExecutionState();
	
		Assert.assertFalse(state.wasStreamKeyContainedInLastQuery("abc"));
		state.addStreamKeyToState("abc");
		Assert.assertTrue(state.wasStreamKeyContainedInLastQuery("abc"));

		Assert.assertTrue(state.removeStreamKeyFromState("abc"));
		Assert.assertFalse(state.removeStreamKeyFromState("abc"));
		Assert.assertFalse(state.wasStreamKeyContainedInLastQuery("abc"));
	}
	
	@Test(timeout = 60_000)
	public void testJoinState() {
		final ContinuousQueryExecutionState state = new ContinuousQueryExecutionState();
	
		state.clearJoinPartnerState();
		state.addJoinCandidateForCurrentKey("abc");
		state.addJoinCandidateForCurrentKey("def");
		
		final Set<String> missingPartners1 = state.clearStateAndGetMissingJoinpartners("stream");
		Assert.assertTrue(missingPartners1.isEmpty());
		
		final Set<String> missingPartners2 = state.clearStateAndGetMissingJoinpartners("stream");
		Assert.assertEquals(2, missingPartners2.size());
		Assert.assertTrue(missingPartners2.contains("abc"));
		Assert.assertTrue(missingPartners2.contains("def"));

		state.clearJoinPartnerState();
		state.addJoinCandidateForCurrentKey("abc");
		state.addJoinCandidateForCurrentKey("def");
		final Set<String> missingPartners3 = state.clearStateAndGetMissingJoinpartners("stream");
		Assert.assertTrue(missingPartners3.isEmpty());
		
		state.addJoinCandidateForCurrentKey("def");
		final Set<String> missingPartners4 = state.clearStateAndGetMissingJoinpartners("stream");
		Assert.assertEquals(1, missingPartners4.size());
		Assert.assertTrue(missingPartners4.contains("abc"));
	}
}
