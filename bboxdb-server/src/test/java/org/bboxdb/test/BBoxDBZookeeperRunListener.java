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
package org.bboxdb.test;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;

/**
 * A JUnit run listener that starts the shared ZooKeeper test container and
 * patches the BBoxDB configuration before the first test of a Maven Surefire
 * run is executed.
 *
 * This is registered via the {@code listener} property of the Surefire plugin.
 * Patching the configuration up front (before any test class is loaded)
 * guarantees that the {@code ZookeeperClientFactory} caches a client that
 * points to the test container, even for tests that reach ZooKeeper
 * transitively without extending {@link BBoxDBTestEnvironment}.
 */
public class BBoxDBZookeeperRunListener extends RunListener {

	@Override
	public void testRunStarted(final Description description) throws Exception {
		BBoxDBTestEnvironment.prepare();
	}
}