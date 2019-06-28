import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DoubleGroupingComparator extends WritableComparator {
    public DoubleGroupingComparator() {
        super(DoubleData.class, true);
    }

    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleData k1 = (DoubleData) w1;
        DoubleData k2 = (DoubleData) w2;
        return k1.getInt2().compareTo(k2.getInt2());
    }
}
