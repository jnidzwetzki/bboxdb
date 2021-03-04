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
package org.bboxdb;

import java.util.Arrays;
import java.util.List;

import org.bboxdb.commons.ExceptionHelper;
import org.junit.Assert;
import org.junit.Test;


public class TestExceptionHelper {
	
	@Test(timeout=60_000)
	public void getStacktraceFromList() {
		final List<Exception> exceptions = Arrays.asList(new IllegalAccessException("Failed 1"), 
				new IllegalAccessException("Failed 2"));
		
		final String stacktrace = ExceptionHelper.getFormatedStacktrace(exceptions);
		System.out.println(stacktrace);
		Assert.assertTrue(stacktrace.contains("Failed 1"));
		Assert.assertTrue(stacktrace.contains("Failed 2"));
	}

}
