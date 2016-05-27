package de.fernunihagen.dna.jkn.scalephant.storage.entity;

public class FloatInterval {
	
	/**
	 * The begin of the interval
	 */
	protected final float begin;
	
	/**
	 * The end of the interval
	 */
	protected final float end;
	
	/**
	 * Begin point included
	 */
	protected final boolean beginIncluded;
	
	/**
	 * End point included
	 */
	protected final boolean endIncluded;
	
	public FloatInterval(final float begin, final float end) {
		this(begin, end, true, true);
	}

	public FloatInterval(final float begin, final float end, final boolean beginIncluded, final boolean endIncluded) {
		super();
		
		if(begin > end) {
			throw new IllegalArgumentException("Failed to construct an interval with: begin > end");
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
	public float getBegin() {
		return begin;
	}

	/**
	 * Get the end of the interval
	 * @return
	 */
	public float getEnd() {
		return end;
	}
	
	/**
	 * Get the middle point of the interval
	 * @return
	 */
	public float getMidpoint() {
		return (float) ((end - begin) / 2.0) + begin;
	}
	
	/**
	 * Is the number covered by the interval?
	 * @param number
	 * @return
	 */
	public boolean isNumberIncluded(final float number) {
		return overlapsWith(number, true);
	}
	
	/**
	 * Is the number covered by the interval?
	 * @param number
	 * @return
	 */
	public boolean overlapsWith(final float number, final boolean numberIncluded) {
		
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
	public boolean isOverlappingWith(final FloatInterval otherInterval) {
		
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
	 * Split the interval at the given position and return the left part
	 * @return
	 */
	public FloatInterval splitAndGetLeftPart(final float splitPosition, final boolean splitPositionIncluded) {
		if(! overlapsWith(splitPosition, true)) {
			throw new IllegalArgumentException("Split position is not included: " + toString() + " / " + splitPosition);
		}
		
		return new FloatInterval(begin, splitPosition, beginIncluded, splitPositionIncluded);
	} 
	
	/**
	 * Split the interval at the given position and return the right part
	 * @return
	 */
	public FloatInterval splitAndGetRightPart(final float splitPosition, final boolean splitPositionIncluded) {
		if(! overlapsWith(splitPosition, true)) {
			throw new IllegalArgumentException("Split position is not included: " + toString() + " / " + splitPosition);
		}
		
		return new FloatInterval(splitPosition, end, splitPositionIncluded, endIncluded);
	} 

	@Override
	public String toString() {
		return "FloatInterval [begin=" + begin + ", end=" + end
				+ ", beginIncluded=" + beginIncluded + ", endIncluded="
				+ endIncluded + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(begin);
		result = prime * result + (beginIncluded ? 1231 : 1237);
		result = prime * result + Float.floatToIntBits(end);
		result = prime * result + (endIncluded ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		
		final FloatInterval other = (FloatInterval) obj;
		
		if(other.isBeginIncluded() != isBeginIncluded()) {
			return false;
		}
		
		if(other.isEndIncluded() != isEndIncluded()) {
			return false;
		}
		
		if(other.getBegin() != getBegin()) {
			return false;
		}
		
		if(other.getEnd() != getEnd()) {
			return false;
		}
		
		return true;
	}
	
}
