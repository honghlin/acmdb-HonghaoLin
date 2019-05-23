package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private TupleDesc tupleDesc;
	
	private Map<Field, Tuple> tuples;
	
	private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

	
	
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	
    	this.gbfield=gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	tuples = new HashMap<>();
    	
    	if (gbfield == -1) tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        else tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    	Field key = gbfield == -1 ? null : tup.getField(gbfield);
        String value = ((StringField) tup.getField(afield)).getValue();
        int v = 0;	
        Tuple t = null;
        if(tuples.containsKey(key))	{
        	t = tuples.get(key);
        	v = ((IntField) t.getField(key == null ? 0 : 1)).getValue();
        }
        else t = new Tuple(tupleDesc);	
        
        if (key != null) {
            t.setField(0, key);
            t.setField(1, new IntField(v + 1));
        } 
        else t.setField(0, new IntField(v + 1));
        
        tuples.put(key, t);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab3");
    	return new TupleIterator(tupleDesc, tuples.values());
    }

}
