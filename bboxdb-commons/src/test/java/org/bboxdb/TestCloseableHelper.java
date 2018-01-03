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

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Consumer;

import org.bboxdb.commons.CloseableHelper;
import org.junit.Test;
import org.mockito.Mockito;

public class TestCloseableHelper {

	@Test
	public void testCloseAutocloseable() throws Exception {
		@SuppressWarnings("unchecked")
		final Consumer<Exception> consumer = Mockito.mock(Consumer.class);
		final AutoCloseable autoCloseable = Mockito.mock(AutoCloseable.class);
		final IOException ioException = new IOException();
		
		CloseableHelper.closeWithoutException(autoCloseable);
		(Mockito.verify(autoCloseable, Mockito.times(1))).close();
		
		CloseableHelper.closeWithoutException(autoCloseable, consumer);
		(Mockito.verify(autoCloseable, Mockito.times(2))).close();
		(Mockito.verify(consumer, Mockito.times(0))).accept(ioException);
		
		Mockito.doThrow(ioException).when(autoCloseable).close();
		CloseableHelper.closeWithoutException(autoCloseable, consumer);
		(Mockito.verify(autoCloseable, Mockito.times(3))).close();
		(Mockito.verify(consumer, Mockito.times(1))).accept(ioException);
	}

	@Test
	public void testCloseCloseable() throws IOException {
		@SuppressWarnings("unchecked")
		final Consumer<Exception> consumer = Mockito.mock(Consumer.class);
		final Closeable coseable = Mockito.mock(Closeable.class);
		final IOException ioException = new IOException();
		
		CloseableHelper.closeWithoutException(coseable);
		(Mockito.verify(coseable, Mockito.times(1))).close();
		
		CloseableHelper.closeWithoutException(coseable, consumer);
		(Mockito.verify(coseable, Mockito.times(2))).close();
		(Mockito.verify(consumer, Mockito.times(0))).accept(ioException);
		
		Mockito.doThrow(ioException).when(coseable).close();
		CloseableHelper.closeWithoutException(coseable, consumer);
		(Mockito.verify(coseable, Mockito.times(3))).close();
		(Mockito.verify(consumer, Mockito.times(1))).accept(ioException);
	}
}
