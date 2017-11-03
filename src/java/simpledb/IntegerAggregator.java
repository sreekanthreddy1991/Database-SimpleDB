package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField, aField;
    private Type gbFieldType;
    private Aggregator.Op op;
    private ArrayList<Tuple> aggTups = new ArrayList<Tuple>();
    private HashMap<Field, Integer> countMap = new HashMap<Field, Integer>();
    private HashMap<Field, Integer> sumMap = new HashMap<Field, Integer>();
    private TupleDesc td;
    private int OutputAggField, OutputGBField;

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
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        if(gbfield != Aggregator.NO_GROUPING){
            Type[] types = new Type[] {gbfieldtype, Type.INT_TYPE};
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Iterator<Tuple> it = aggTups.iterator();

        while (it.hasNext()) {
            Tuple t = it.next();
            if(gbField == Aggregator.NO_GROUPING) {
                int val = ( (IntField) t.getField(OutputAggField)).getValue();
                IntField newField = new IntField(AggValue(val, tup));
                t.setField(OutputAggField, newField);
                return;
            }

            Field f1 = t.getField(OutputGBField);
            Field f2 = tup.getField(gbField);
            if (f1.equals(f2)) {
                int val = ((IntField) t.getField(OutputAggField)).getValue();
                IntField newField = new IntField(AggValue(val, tup));
                t.setField(OutputAggField, newField);
                return;
            }
        }

        /* Array of tuples is empty or does not have the group by element */
        Tuple t = new Tuple(td);
        Field initialAggField, grpField;
        grpField = new IntField(-1);
        if (op == Op.COUNT)
            initialAggField = new IntField(1);
        else
            initialAggField = tup.getField(aField);

        if (gbField != Aggregator.NO_GROUPING){
            grpField = tup.getField(gbField);
            t.setField(OutputGBField, grpField);
            t.setField(OutputAggField, initialAggField);
        }
        else
            t.setField(OutputAggField, initialAggField);

        aggTups.add(t);
        countMap.put(grpField, 1);
        sumMap.put(grpField, ((IntField)tup.getField(aField)).getValue());
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
        return new TupleIterator(td, aggTups);
    }

    private int AggValue(int existingValue, Tuple tup){
        int currentValue = ((IntField) tup.getField(aField)).getValue();
        switch(op) {
            case COUNT:
                return existingValue + 1;
            case MIN:
                return existingValue < currentValue ? existingValue : currentValue;
            case MAX:
                return existingValue > currentValue ? existingValue : currentValue;
            case SUM:
                return existingValue + currentValue;
            case AVG:
                Field tupField;
                if (gbField != Aggregator.NO_GROUPING)
                    tupField = tup.getField(gbField);
                else
                    tupField = new IntField(-1);
                int curCount = countMap.get(tupField) + 1;
                int curSum = sumMap.get(tupField) + currentValue;
                countMap.put(tupField, curCount);
                sumMap.put(tupField, curSum);
                return curSum/curCount;
        }
        return -1;
    }

}
