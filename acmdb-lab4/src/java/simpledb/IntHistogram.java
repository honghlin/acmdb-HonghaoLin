package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
	
	private int buckets;
	private int min;
	private int max;
	private int width;
	private int ntups;
    private int[] bucket;
	
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (max - min + 1) / buckets;
        if((max - min + 1) % buckets != 0) width += 1;
        if(width == 0) width += 1;
        this.ntups = 0;
        this.bucket = new int[buckets]; 
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here

    	ntups += 1;
    	int idx =  (v - min) / width;
    	bucket[idx] += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
    	
    	int idx = (v - min) / width;
    	
    	switch(op) {
        case EQUALS:
            
            if (v < min || v > max) return 0.0;
            //System.out.println(v);
            return ((double)bucket[idx] / width) / ntups;
            
        case NOT_EQUALS:

            if (v < min || v > max) return 1.0;
            idx = (v - min) / width;
            return 1.0 - ((double)bucket[idx] / width) / ntups;
            
        case GREATER_THAN:

            if (v < min) return 1.0;
            if (v > max) return 0.0;
            int b_right = min + (idx + 1) * width - 1;
            double ans = 0.0;
            ans += ((double)(b_right - v) / width) * bucket[idx];
            for (int i = idx + 1; i < buckets; ++i) ans += (double)bucket[i];
            return ans / ntups;
            
        case LESS_THAN:

            if (v < min) return 0.0;
            if (v > max) return 1.0;
            int b_left = min + idx * width;
            ans = 0.0;
            ans += ((double)(v - b_left) / width) * bucket[idx];
            for (int i = 0; i < idx; ++i) ans += (double)bucket[i];
            return ans / ntups; 
            
        case GREATER_THAN_OR_EQ:

            if (v < min) return 1.0;
            if (v > max) return 0.0;
            b_right = min + (idx + 1) * width - 1;
            ans = (double)bucket[idx] / width;
            ans += ((double)(b_right - v) / width) * bucket[idx];
            for (int i = idx + 1; i < buckets; ++i) ans += (double)bucket[i];
            return ans / ntups;
            
        case LESS_THAN_OR_EQ:
            
            if (v < min) return 0.0;
            if (v > max) return 1.0;
            b_left = min + idx * width;
            ans = (double)bucket[idx] / width;
            ans += ((double)(v - b_left) / width) * bucket[idx];
            for (int i = 0; i < idx; ++i) ans += (double)bucket[i];
            return ans / ntups; 
            
       }
    	
        return -1.0;//
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        //return null;
    	return "IntHistogram";
    }
}
