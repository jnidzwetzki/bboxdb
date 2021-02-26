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
package org.bboxdb.commons.math;

import org.bboxdb.commons.InputParseException;
import org.bboxdb.commons.MathUtil;
import org.bboxdb.commons.StringUtil;

public class DoubleInterval implements Comparable<DoubleInterval> {
	
	/**
	 * The begin of the interval
	 */
	private final double begin;
	
	/**
	 * The end of the interval
	 */
	private final double end;
	
	/**
	 * Begin point included
	 */
	private final boolean beginIncluded;
	
	/**
	 * End point included
	 */
	private final boolean endIncluded;
	
	/**
	 * The min value
	 */
	public final static double MIN_VALUE = -Float.MAX_VALUE;
	
	/**
	 * The max value
	 */
	public final static double MAX_VALUE = Float.MAX_VALUE;
	
	
	public DoubleInterval(final double begin, final double end) {
		this(begin, end, true, true);
	}
	
	public DoubleInterval(final String stringValue) {
		
		if(! (stringValue.startsWith("[") || stringValue.startsWith("("))) {
			throw new IllegalArgumentException("Interval have to start with ( or [");
		}
		
		if(! (stringValue.endsWith("]") || stringValue.endsWith(")"))) {
			throw new IllegalArgumentException("Interval have to end with ) or ]");
		}

		if(StringUtil.countCharOccurrence(stringValue, ',') != 1) {
			throw new IllegalArgumentException("Interval have to contain exactly one ','");
		}
		
		this.beginIncluded = stringValue.startsWith("[") ? true : false;
		this.endIncluded = stringValue.endsWith("]") ? true : false;
		
		final String[] beginEnd = stringValue.substring(1, stringValue.length() - 1).split(",");
		
		try {
			if("min".equals(beginEnd[0])) {
				this.begin = MIN_VALUE;
			} else {
				this.begin = MathUtil.tryParseDouble(beginEnd[0], () -> "Unable to parse: " + beginEnd[0]);
			}
			
			if("max".equals(beginEnd[1])) {
				this.end = MAX_VALUE;
			} else {
				this.end = MathUtil.tryParseDouble(beginEnd[1], () -> "Unable to parse: " + beginEnd[1]);
			}
		} catch (InputParseException e) {
			throw new IllegalArgumentException(e);
		}		
	}

	public DoubleInterval(final double begin, final double end, final boolean beginIncluded, final boolean endIncluded) {
		
		if(begin > end) {
			throw new IllegalArgumentException("Failed to construct an interval with: begin " 
					+ begin + " > end " + end);
		}
		
		if(begin == end) {
			if(beginIncluded == false || endIncluded == false) {
				throw new IllegalArgumentException("Unable to construct open interval with: begin = end");
			}
		}
		
		this.begin = begin;
		this.end = end;
		this.beginIncluded = beginIncluded;
		this.endIncluded = endIncluded;
	}

	/**
	 * Is the begin included?
	 * @return
	 */
	public boolean isBeginIncluded() {
		return beginIncluded;
	}

	/**
	 * Is the end point included
	 * @return
	 */
	public boolean isEndIncluded() {
		return endIncluded;
	}

	/**
	 * Get the begin of the interval
	 * @return
	 */
	public double getBegin() {
		return begin;
	}

	/**
	 * Get the end of the interval
	 * @return
	 */
	public double getEnd() {
		return end;
	}
	
	/**
	 * Get the middle point of the interval
	 * @return
	 */
	public double getMidpoint() {
		return (getLength() / 2.0) + begin;
	}

	/**
	 * Get the length of the interval
	 * @return
	 */
	public double getLength() {
		return end - begin;
	}
	
	/**
	 * Is the number covered by the interval?
	 * @param number
	 * @return
	 */
	public boolean isPointIncluded(final double number) {
		return overlapsWith(number, true);
	}
	
	/**
	 * Is the number covered by the interval?
	 * @param number
	 * @return
	 */
	public boolean overlapsWith(final double number, final boolean numberIncluded) {
		
		boolean betweenBeginAndEnd = (number >= begin && number <= end);
		
		if(betweenBeginAndEnd == false) {
			return false;
		}
		
		// Refine result by checking begin and endpoint
		if(number == begin) {
			if(beginIncluded == false) {
				return false;
			}
			
			if(numberIncluded == false) {
				return false;
			}
		}
		
		if(number == end) {
			if(endIncluded == false) {
				return false;
			}
			
			if(numberIncluded == false) {
				return false;
			}
		}

		return true;
	}
	
	
	/**
	 * Does this interval overlaps with an other interval?
	 * 
	 * 	 * Case 1: 1 overlaps 2 at the left end
	 *  |--------|                      // 1
	 *      |------------|              // 2
	 *
	 * Case 2: 1 overlaps 2 at the tight end
	 *            |--------|            // 1
	 *   |------------|                 // 2
	 *
	 * Case 3: 1 is inside 2
	 *    |-------------------|         // 1
	 *  |-----------------------|       // 2
	 *
	 * Case 4: 2 is inside 1
	 * |-----------------------|        // 1
	 *      |----------|                // 2
	 *
	 * Case 5: 1 = 2
	 *            |--------|            // 1
	 *            |--------|            // 2
	 *
	 * Case 6: Interval end overlapping
	 *      |--------|                  // 1
	 *               |--------|         // 2
	 *                        
	 * Case 7: No overlapping
	 * |-------|                        // 1
	 *               |---------|        // 2
	 * 
	 * @param otherInterval
	 * @return
	 */
	public boolean isOverlappingWith(final DoubleInterval otherInterval) {
		
		// Case 1 and 6
		if(overlapsWith(otherInterval.getBegin(), otherInterval.isBeginIncluded())) {
			return true;
		}
		
		// Case 2 and 6
		if(overlapsWith(otherInterval.getEnd(), otherInterval.isEndIncluded())) {
			return true;
		}
		
		// Case 3 and 4
		if(otherInterval.overlapsWith(begin, beginIncluded)) {
			return true;
		}
		
		// Case 5:
		if(otherInterval.equals(this)) {
			return true;
		}

		return false;
	}
	
	/**
	 * Is the other interval completely covered by this interval?
	 * @param otherInterval
	 * @return
	 */
	public boolean isCovering(final DoubleInterval otherInterval) {
		if(otherInterval.getBegin() < getBegin()) {
			return false;
		}
		
		if(otherInterval.getEnd() > getEnd()) {
			return false;
		}
		
		if(otherInterval.getBegin() == getBegin()) {
			if(isBeginIncluded() == false && otherInterval.isBeginIncluded() == true) {
				return false;
			}
		}
		
		if(otherInterval.getEnd() == getEnd()) {
			if(isEndIncluded() == false && otherInterval.isEndIncluded() == true) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Returns the overlapping interval or null, if both
	 * intervals does not overlap
	 * 
	 * @param otherInterval
	 * @return 
	 */
	public DoubleInterval getIntersection(final DoubleInterval otherInterval) {
		
		if(otherInterval == null) {
			return null;
		}
		
		if(equals(otherInterval)) {
			return this;
		}
		
		// We are overlapping the complete interval
		if(getBegin() < otherInterval.getBegin() && getEnd() > otherInterval.getEnd()) {
			return otherInterval;
		}
		
		// The other interval overlaps us
		if(otherInterval.getBegin() < getBegin() && otherInterval.getEnd() > getEnd()) {
			return this;
		}
		
		// Not overlapping
		if(! isOverlappingWith(otherInterval)) {
			return null;
		}
		
		// Left overlapping
		if(getBegin() < otherInterval.getBegin()) {
			
			if(otherInterval.getBegin() == end 
					&& otherInterval.isBeginIncluded() == false 
					&& endIncluded == false) {
				return null;
			}

			return new DoubleInterval(otherInterval.getBegin(), end, otherInterval.isBeginIncluded(), endIncluded);
		}
		
		if(begin == otherInterval.getEnd() 
				&& beginIncluded == false 
				&& otherInterval.isEndIncluded() == false) {
			return null;
		}
		
		if(beginIncluded && otherInterval.beginIncluded) {
			if(begin == otherInterval.getBegin()) {
				return new DoubleInterval(begin, Math.min(getEnd(), otherInterval.getEnd()), true, true);
			}
		}
	
		return new DoubleInterval(begin, otherInterval.getEnd(), beginIncluded, otherInterval.isEndIncluded());
	}
	
	/**
	 * Split the interval at the given position and return the left part
	 * @return
	 */
	public DoubleInterval splitAndGetLeftPart(final double splitPosition, final boolean splitPositionIncluded) {
		if(! overlapsWith(splitPosition, true)) {
			throw new IllegalArgumentException("Split position is not included: " + toString() + " / " + splitPosition);
		}
		
		return new DoubleInterval(begin, splitPosition, beginIncluded, splitPositionIncluded);
	} 
	
	/**
	 * Split the interval at the given position and return the right part
	 * @return
	 */
	public DoubleInterval splitAndGetRightPart(final double splitPosition, final boolean splitPositionIncluded) {
		if(! overlapsWith(splitPosition, true)) {
			throw new IllegalArgumentException("Split position is not included: " + toString() + " / " + splitPosition);
		}
		
		return new DoubleInterval(splitPosition, end, splitPositionIncluded, endIncluded);
	} 

	@Override
	public String toString() {
		return getRoundedString(0);
	}

	/**
	 * Get the rounded string value
	 * @return
	 */
	public String getRoundedString(final int precision) {
		final StringBuffer sb = new StringBuffer();
		
		if(beginIncluded) {
			sb.append("[");
		} else {
			sb.append("(");
		}
		
		if(begin == MIN_VALUE) {
			sb.append("min");
		} else {
			sb.append(getRoundedValue(begin, precision));	
		}
		
		sb.append(",");
		
		if(end == MAX_VALUE) {
			sb.append("max");
		} else {
			sb.append(getRoundedValue(end, precision));	
		}
				
		if(endIncluded) {
			sb.append("]");
		} else {
			sb.append(")");	
		}
		
		return sb.toString();
	}
	
	/**
	 * Get the rounded value
	 * @param value
	 * @param precision
	 * @return
	 */
	private double getRoundedValue(final double value, final int precision) {
		if(precision == 0) {
			return value;
		}
		
		return MathUtil.round(value, precision);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(begin);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (beginIncluded ? 1231 : 1237);
		temp = Double.doubleToLongBits(end);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + (endIncluded ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DoubleInterval other = (DoubleInterval) obj;
		if (Double.doubleToLongBits(begin) != Double
				.doubleToLongBits(other.begin))
			return false;
		if (beginIncluded != other.beginIncluded)
			return false;
		if (Double.doubleToLongBits(end) != Double.doubleToLongBits(other.end))
			return false;
		if (endIncluded != other.endIncluded)
			return false;
		return true;
	}

	@Override
	public int compareTo(final DoubleInterval otherInterval) {
		if(getBegin() != otherInterval.getBegin()) {
			return Double.compare(getBegin(), otherInterval.getBegin());
		}
		
		return Double.compare(getEnd(), otherInterval.getEnd());
	}
	
}
