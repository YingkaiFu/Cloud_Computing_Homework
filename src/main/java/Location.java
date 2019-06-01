import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Location implements WritableComparable<Location> {

    private DoubleWritable longitude, latitude;

    public Location(DoubleWritable longitude, DoubleWritable latitude) {
        this.longitude = longitude;
        this.latitude = latitude;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //写入流
        longitude.write(dataOutput);
        latitude.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //读取流
        longitude.readFields(dataInput);
        latitude.readFields(dataInput);
    }

    @Override
    public int compareTo(Location o) {
        if (longitude.compareTo(o.longitude) == 0) {
            return latitude.compareTo(o.latitude);
        } else return longitude.compareTo(o.longitude);
    }
}
