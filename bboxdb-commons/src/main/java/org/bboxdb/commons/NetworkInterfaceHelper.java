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
package org.bboxdb.commons;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.stream.Collectors;

public class NetworkInterfaceHelper {

	/**
	 * Get a list with non loopback ipv4 ips
	 * @return
	 * @throws SocketException 
	 */
	public static List<Inet4Address> getNonLoopbackIPv4() throws SocketException {
		final List<InetAddress> allAddresses = new ArrayList<>();
		
	    final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
	    
	    if(interfaces == null) {
	    		return new ArrayList<>();
	    }
	    
	    for(final NetworkInterface iface : Collections.list(interfaces)) {
	        final Enumeration<InetAddress> inetAddresses = iface.getInetAddresses();
	        allAddresses.addAll(Collections.list(inetAddresses));
	    }
	    
	    return allAddresses
	    	.stream()
	    	.filter(i -> i instanceof Inet4Address)
	    	.filter(i -> ! i.isLoopbackAddress())
	    	.filter(i -> ! i.isLinkLocalAddress())
	    	.map(i -> (Inet4Address) i)
	    	.collect(Collectors.toList());
	}
	
	/**
	 * Get the first non loopback v4 ip address as string
	 * @return
	 * @throws SocketException 
	 */
	public static Inet4Address getFirstNonLoopbackIPv4() throws SocketException {
		final List<Inet4Address> allAddresses = getNonLoopbackIPv4();
		
		if(allAddresses.isEmpty()) {
			throw new SocketException("No ipv4 ips found");
		}
		
		return allAddresses.get(0);
	}
	
	/**
	 * Get the first non loopback v4 ip address as string
	 * @return
	 * @throws SocketException 
	 */
	public static String getFirstNonLoopbackIPv4AsString() throws SocketException {
		return getFirstNonLoopbackIPv4().getHostAddress();
	}

}
