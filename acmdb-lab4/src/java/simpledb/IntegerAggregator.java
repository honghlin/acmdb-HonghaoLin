package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;
    
    private Map<Field, Integer> Count, Min, Max, Sum;
    private Map<Field, Tuple> Tuples;

    
	private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	
    	this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == -1) tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        else tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        
        Count = new HashMap<>();
        Sum = new HashMap<>();
        Max = new HashMap<>();
        Min = new HashMap<>();
        Tuples = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	Field key = gbfield == -1 ? null : tup.getField(gbfield);
        int value = ((IntField) tup.getField(afield)).getValue();
        int ans = 0;  
        switch (what) {
        case AVG:
            
        	if(!Count.containsKey(key)) Count.put(key, 0);
        	if(!Sum.containsKey(key)) Sum.put(key, 0);
        	Count.put(key, Count.get(key) + 1);
        	Sum.put(key, Sum.get(key) + value);
            ans = Sum.get(key) / Count.get(key);
            break;
        case COUNT:
            
        	if(!Count.containsKey(key)) Count.put(key, 0);
        	Count.put(key, Count.get(key) + 1);
            ans = Count.get(key);
            break;
        case SUM:
            
        	if(!Sum.containsKey(key)) Sum.put(key, 0);
        	Sum.put(key, Sum.get(key) + value);
        	ans = Sum.get(key);
            break;
        case MIN:
            
        	if(!Min.containsKey(key)) Min.put(key, Integer.MAX_VALUE);
        	Min.put(key, Math.min(value, Min.get(key)));
            ans = Min.get(key);
            break;
        case MAX:
            
        	if(!Max.containsKey(key)) Max.put(key, Integer.MIN_VALUE);
        	Max.put(key, Math.max(value, Max.get(key)));    
            ans = Max.get(key);  
            break;
        }      
        Tuple t = new Tuple(tupleDesc);
        if (gbfield == -1) t.setField(0, new IntField(ans));
        else {
            t.setField(0, key);
            t.setField(1, new IntField(ans));
        } 
        Tuples.put(key, t);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab3");
    	return new TupleIterator(tupleDesc, Tuples.values());
    }

}
