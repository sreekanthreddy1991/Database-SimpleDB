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
    private TupleDesc td;
    private int OutputAggField, OutputGBField;

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
        if(gbfield != Aggregator.NO_GROUPING){
            Type[] types = new Type[] {gbFieldType, Type.INT_TYPE};
            td = new TupleDesc(types);
            OutputAggField = 1;
            OutputGBField = 0;
        }
        else{
            Type[] types = new Type[] {Type.INT_TYPE};
            td = new TupleDesc(types);
            OutputAggField = 0;
            OutputGBField = -1;
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

            if(gbField == Aggregator.NO_GROUPING) {
                int val = ( (IntField) t.getField(OutputAggField)).getValue();
                IntField newField = new IntField(AggValue(val));
                t.setField(OutputAggField, newField);
                return;
            }

            Field f1 = t.getField(OutputGBField);
            Field f2 = tup.getField(gbField);
            if (f1.equals(f2)) {
                int val = ((IntField) t.getField(OutputAggField)).getValue();
                IntField newField = new IntField(AggValue(val));
                t.setField(OutputAggField, newField);
                return;
            }
        }

        /* Array of tuples is empty or does not have the group by element */
        Tuple t = new Tuple(td);
        Field initialField;
        if (op == Op.COUNT)
            initialField = new IntField(1);
        else
            initialField = tup.getField(aField);
        if (gbField != Aggregator.NO_GROUPING){
            t.setField(OutputGBField, tup.getField(gbField));
            t.setField(OutputAggField, initialField);
        }
        else{
            t.setField(OutputAggField, initialField);
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

    private int AggValue(int existingValue){
        switch(op) {
            case COUNT:
                return existingValue + 1;
        }
        return -1;
    }
}
