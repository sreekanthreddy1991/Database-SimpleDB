package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField, aField;
    private Type gbFieldType;
    private Aggregator.Op op;
    private ArrayList<Tuple> aggTups = new ArrayList<Tuple>();
    private HashSet<Field> elementsInAggTups = new HashSet<Field>();
    private HashMap<Field, Integer> countMap = new HashMap<Field, Integer>();
    private HashMap<Field, Integer> sumMap = new HashMap<Field, Integer>();
    private TupleDesc td;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        if(gbfield != -1){
            Type[] types = new Type[] {Type.INT_TYPE, Type.INT_TYPE};
            td = new TupleDesc(types);
        }
        else{
            Type[] types = new Type[] {Type.INT_TYPE};
            td = new TupleDesc(types);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Iterator<Tuple> it = aggTups.iterator();

        while (it.hasNext()) {
            Tuple t = it.next();
            if(gbField == -1) {
                int val3 = ( (IntField) t.getField(0) ).getValue();
                int aggValue = AggValue(val3);
                IntField newField = new IntField(aggValue);
                t.setField(0, newField);
                return;
            }

            int val1 = ((IntField) t.getField(0)).getValue();
            int val2 = ((IntField) tup.getField(gbField)).getValue();
            if (val1 == val2) {
                int val3 = ((IntField) t.getField(1)).getValue();
                int aggValue = AggValue(val3);
                IntField newField = new IntField(aggValue);
                t.setField(1, newField);
                return;
            }
        }

        /* Array of tuples is empty or does not have the group by element */
        Tuple t = new Tuple(td);
        IntField tempField = new IntField(1);
        if (gbField != -1){
            t.setField(0, tup.getField(gbField));
            t.setField(1, tempField);
            elementsInAggTups.add(tup.getField(gbField));
        }
        else{
            t.setField(0,tempField);
        }
        aggTups.add(t);
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
        return new TupleIterator(td, aggTups);
    }

    private boolean CheckIfGBElementExists(Tuple tup) {
        if (gbField == -1) {
            return true;
        }

        if (elementsInAggTups.contains(tup.getField(gbField))) {
            return true;
        }
        return false;
    }

    private int AggValue(int existingValue){
        switch(op) {
            case COUNT:
                return existingValue + 1;
        }
        return -1;
    }
}
