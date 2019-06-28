import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleData implements WritableComparable<DoubleData> {
    private LongWritable int1;
    private LongWritable int2;

    DoubleData(LongWritable int1, LongWritable int2) {
        this.int1 = int1;
        this.int2 = int2;
    }

    DoubleData() {
        this.int1 = new LongWritable();
        this.int2 = new LongWritable();
    }

    public LongWritable getInt1() {
        return int1;
    }

    public void setInt1(LongWritable int1) {
        this.int1 = int1;
    }

    public LongWritable getInt2() {
        return int2;
    }

    public void setInt2(LongWritable int2) {
        this.int2 = int2;
    }

    public int compareTo(DoubleData o) {
        int compareValue = this.int2.compareTo(o.int2);
        if (compareValue == 0) {
            compareValue = this.int1.compareTo(o.int1);
        }
        return compareValue;
    }

    public void write(DataOutput dataOutput) throws IOException {
        int1.write(dataOutput);
        int2.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.int1.readFields(dataInput);
        this.int2.readFields(dataInput);
    }
}
