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
package org.bboxdb.networkproxy;

public class ProxyConst {

	/**
	 * The proxy port
	 */
	public final static int PROXY_PORT = 10051;

	/**
	 * Result - OK
	 */
	public final static byte RESULT_OK = 0x00;

	/**
	 * Result - failed
	 */
	public final static byte RESULT_FAILED = 0x01;

	/**
	 * Result - follow
	 */
	public final static byte RESULT_FOLLOW = 0x02;

	/**
	 * Command - put
	 */
	public final static byte COMMAND_PUT = 0x01;

	/**
	 * Command - get
	 */
	public final static byte COMMAND_GET = 0x02;

	/**
	 * Command - delete
	 */
	public final static byte COMMAND_DELETE = 0x03;

	/**
	 * Command - get local data
	 */
	public final static byte COMMAND_GET_LOCAL = 0x04;

	/**
	 * Command - get local data
	 */
	public final static byte COMMAND_RANGE_QUERY = 0x05;

	/**
	 * Command - close
	 */
	public final static byte COMMAND_CLOSE = 0x06;

}
