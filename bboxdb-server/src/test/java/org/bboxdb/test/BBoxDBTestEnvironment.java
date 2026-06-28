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

import java.util.Collections;

import org.bboxdb.misc.BBoxDBConfigurationManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for all tests that need a running ZooKeeper instance.
 *
 * A single ZooKeeper container is started (via Testcontainers) for the whole
 * JVM and shared between all test classes. The dynamically assigned address of
 * the container is injected into the BBoxDB configuration, so that all
 * components that read the ZooKeeper nodes from the configuration connect to
 * the container.
 *
 * The container is prepared from two places to make the setup robust:
 *
 * <ul>
 *   <li>{@link BBoxDBZookeeperRunListener} calls {@link #prepare()} before the
 *       first test of a Maven Surefire run. This guarantees that the
 *       configuration is patched before <em>any</em> test (even ones that do
 *       not extend this class but reach ZooKeeper transitively) creates the
 *       cached {@code ZookeeperClient}.</li>
 *   <li>The static initializer of this class calls {@link #prepare()} as well,
 *       so that running a single test that extends this class (e.g. from an
 *       IDE) also works without the Surefire listener.</li>
 * </ul>
 *
 * The container is not stopped explicitly; the Testcontainers Ryuk component
 * removes it once the JVM terminates.
 */
public abstract class BBoxDBTestEnvironment {

	/**
	 * The client port of the ZooKeeper container
	 */
	private static final int ZOOKEEPER_PORT = 2181;

	/**
	 * The ZooKeeper Docker image
	 */
	private static final DockerImageName ZOOKEEPER_IMAGE = DockerImageName.parse("zookeeper:3.9");

	/**
	 * The shared ZooKeeper container. It lives for the whole JVM and is reaped
	 * by the Testcontainers Ryuk component on JVM shutdown.
	 */
	@SuppressWarnings("resource")
	private static final GenericContainer<?> ZOOKEEPER_CONTAINER =
			new GenericContainer<>(ZOOKEEPER_IMAGE)
					.withExposedPorts(ZOOKEEPER_PORT)
					.waitingFor(Wait.forListeningPort());

	/**
	 * Has the environment already been prepared?
	 */
	private static boolean prepared = false;

	static {
		prepare();
	}

	/**
	 * Start the shared ZooKeeper container (if needed) and inject its address
	 * into the BBoxDB configuration. The method is idempotent and may be called
	 * multiple times.
	 */
	public static synchronized void prepare() {
		if(prepared) {
			return;
		}

		if(! ZOOKEEPER_CONTAINER.isRunning()) {
			ZOOKEEPER_CONTAINER.start();
		}

		BBoxDBConfigurationManager.getConfiguration()
				.setZookeepernodes(Collections.singletonList(getZookeeperConnectString()));

		prepared = true;
	}

	/**
	 * Get the connect string (host:port) of the shared ZooKeeper container.
	 *
	 * @return the ZooKeeper connect string
	 */
	public static String getZookeeperConnectString() {
		return ZOOKEEPER_CONTAINER.getHost()
				+ ":" + ZOOKEEPER_CONTAINER.getMappedPort(ZOOKEEPER_PORT);
	}
}
