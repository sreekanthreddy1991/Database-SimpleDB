package simpledb;

import java.awt.geom.QuadCurve2D;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.lang.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private double numBuckets, min, max, width;
    private int numTups = 0;
    private TreeMap<Integer,Integer> histMap = new TreeMap<Integer, Integer>();

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
    public IntHistogram(int buckets, int min, int max) {
    	numBuckets = buckets;
    	this.min = min;
    	this.max = max;
        this.width = (max - min + 1)/numBuckets;
    	int bucketNum = 0;
    	while(bucketNum < buckets){
    	    histMap.put(bucketNum, 0);
    	    bucketNum++;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	double valueToAdd = v;
    	int bucketNum = (int)((valueToAdd - this.min)/this.width);
    	if(bucketNum > this.numBuckets - 1 || bucketNum < 0) {
            return;
        }
       	int currentBucketCount = histMap.get(bucketNum);
    	histMap.put(bucketNum, currentBucketCount + 1);
    	numTups++;
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
        double valueToSearch = v;
        int relevantBucket = (int)((valueToSearch - this.min)/this.width);

        switch(op){
            case EQUALS:
                return selectivityForEquality(relevantBucket);
            case NOT_EQUALS:
                return 1 - selectivityForEquality(relevantBucket);
            case GREATER_THAN:
                return selectivityForGreaterThan(relevantBucket, valueToSearch);
            case GREATER_THAN_OR_EQ:
                return selectivityForEquality(relevantBucket) + selectivityForGreaterThan(relevantBucket, valueToSearch);
            case LESS_THAN:
                return 1 - (selectivityForEquality(relevantBucket) + selectivityForGreaterThan(relevantBucket, valueToSearch));
            case LESS_THAN_OR_EQ:
                return 1 - selectivityForGreaterThan(relevantBucket, valueToSearch);
        }
        return 0;
    }

    private double selectivityForEquality(int relevantBucket){
        if(histMap.get(relevantBucket) == null){
            return 0; // No bucket found
        }
        int heightOfBucket = histMap.get(relevantBucket);
        double result =  (heightOfBucket/this.width)/numTups;
        return result;
    }

    private double selectivityForGreaterThan(int relevantBucket, double v){
        if (relevantBucket > this.numBuckets - 1)
            return 0;
        NavigableMap<Integer,Integer> lesserMap = histMap.headMap(relevantBucket, true);
        int numBuckets = lesserMap.size();
        double rightMostWidth = (numBuckets)*this.width;
        double relevantWidth = Math.min((rightMostWidth - v)/this.width, this.width);
        double selectivity = selectivityForEquality(relevantBucket) * relevantWidth;
        NavigableMap<Integer,Integer> greaterMap = histMap.tailMap(relevantBucket, false);
        for(Map.Entry<Integer, Integer> entry : greaterMap.entrySet()){
            selectivity += selectivityForEquality(entry.getKey());
        }
        return selectivity;
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
        return null;
    }
}
