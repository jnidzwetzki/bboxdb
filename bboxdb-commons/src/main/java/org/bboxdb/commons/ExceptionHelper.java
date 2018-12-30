package org.bboxdb.commons;

import java.util.List;

public class ExceptionHelper {
	
	/**
	 * Get a list of all stacktraces
	 * @param traces
	 * @return
	 */
	public static String getFormatedStacktrace(final List<? extends Throwable> traces) {
		final StringBuilder sb = new StringBuilder();

		for(int i = 0; i < traces.size(); i++) {
			final Throwable element = traces.get(i);

			sb.append("Exception: ");
			sb.append(i);
			sb.append("\n");
			
			sb.append("Message: ");
			sb.append(traces.get(i).getMessage());
			sb.append("\n");
			
			final StackTraceElement[] trace = element.getStackTrace();
			sb.append(StacktraceHelper.getFormatedStacktrace(trace));
		}
		
		return sb.toString();
	}
}
