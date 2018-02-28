/*******************************************************************************
 *
 *    Copyright (C) 2015-2018 the BBoxDB project
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
package org.bboxdb.jmx;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.bboxdb.BBoxDBMain;
import org.bboxdb.misc.BBoxDBService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JMXService implements BBoxDBService {

	/**
	 * The name of the lifecycle mbean
	 */
	public static final String MBEAN_LIFECYCLE = "org.bboxdb:type=LifecycleManager";

	/**
	 * The instance of the application
	 */
	private final BBoxDBMain bBoxDBMain;

	/**
	 * The logger
	 */
	private final static Logger logger = LoggerFactory.getLogger(JMXService.class);

	public JMXService(final BBoxDBMain bboxDBMain) {
		this.bBoxDBMain = bboxDBMain;
	}

	@Override
	public void init() {
		// Register lifecycle mbean
		final LifecycleMBean monitor = new Lifecycle(bBoxDBMain);
		registerBean(monitor, MBEAN_LIFECYCLE);
	}

	/**
	 * Register a new MBean
	 * @param bean
	 * @param beanName
	 */
	private void registerBean(final Object bean, final String beanName) {
		try {
		
			final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
			final ObjectName name = new ObjectName(beanName);

			if (server.isRegistered(name)) {
				logger.debug("MBean {} is already registered", name);
				return;
			}
			
			server.registerMBean(bean, name);
		} catch (Exception e) {
			logger.warn("Got exception while creating mbean", e);
		}
	}

	@Override
	public void shutdown() {
		// Nothing to do
	}

	@Override
	public String getServicename() {
		return "JMX service";
	}
}