/*******************************************************************************
 *
 *    Copyright (C) 2015-2020 the BBoxDB project
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
package org.bboxdb.network.client.future.client;

public enum FutureRetryPolicy {

	// On failure, no future is retried
	RETRY_POLICY_NONE,

	// On failure, the failing future is retried
	RETRY_POLICY_ONE_FUTURE,

	// On failure, all futures (even the successful ones) are retried
	RETRY_POLICY_ALL_FUTURES;
}
