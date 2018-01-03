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
package org.bboxdb.tools.gui;

import javax.swing.UIManager;
import javax.swing.UIManager.LookAndFeelInfo;


public class Main {
	
	/**
	 * Main Method 
	 * @param args
	 * @throws Exception 
	 */
	public static void main(final String[] args) throws Exception {
		
		setLookAndFeel();
		
		final ConnectDialog connectDialog = new ConnectDialog();
		connectDialog.showDialog();
	}

	/**
	 * Try to set the new Nimbus L&F
	 */
	protected static void setLookAndFeel() {
		try {
		    for (final LookAndFeelInfo info : UIManager.getInstalledLookAndFeels()) {
		        if ("Nimbus".equals(info.getName())) {
		            UIManager.setLookAndFeel(info.getClassName());
		            return;
		        }
		    }
		} catch (Exception e) {
		    try {
		        UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
		    } catch (Exception e1) {
		    	// Ignore exception and use the old look and feel
		    }
		}
	}
}
