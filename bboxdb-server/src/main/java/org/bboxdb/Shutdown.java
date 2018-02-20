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
package org.bboxdb;

import java.util.HashMap;
import java.util.Map;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.bboxdb.commons.MathUtil;
import org.bboxdb.jmx.JMXService;
import org.bboxdb.jmx.LifecycleMBean;

/**
 * Shutdown the BBoxDB server via JMX
 * 
 */
public class Shutdown {

	/**
	 * Call the JMX service to shutdown the application
	 * 
	 * @param args
	 */
	public static void main(final String[] args) throws Exception {

		if(args.length != 2) {
			System.err.println("Usage: Shutdown <Port> <Password>");
			System.exit(-1);
		}

		final int jmxPort = MathUtil.tryParseIntOrExit(args[0], 
				() -> args[0] + " is not a valid port");
		
		final String password = args[1];

		// Set username and password
		final Map<String, String[]> env = new HashMap<>();
		final String[] credentials = { "controlRoleUser", password };
		env.put(JMXConnector.CREDENTIALS, credentials);

		// Connect to JMX
		System.out.println("Connecting to JMX shutdown service.");
		final JMXServiceURL url = new JMXServiceURL(
				"service:jmx:rmi:///jndi/rmi://:" + jmxPort + "/jmxrmi");

		final JMXConnector jmxc = JMXConnectorFactory.connect(url, env);
		final MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

		final ObjectName mbeanName = new ObjectName(JMXService.MBEAN_LIFECYCLE);
		final LifecycleMBean mbeanProxy = JMX.newMBeanProxy(mbsc, mbeanName,
				LifecycleMBean.class, true);

		try {
			mbeanProxy.shutdown();
			jmxc.close();
		} catch (Exception e) {
			// Server performs shutdown, JMX connection will be terminated
			// So, ignore exception
		}		
	}
}
