package it.unipi.hadoop;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(DoubleWritable.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable k1 = (DoubleWritable)w1;
        DoubleWritable k2 = (DoubleWritable)w2;

        int result = -1* k1.compareTo(k2);

        return result;
    }
}
