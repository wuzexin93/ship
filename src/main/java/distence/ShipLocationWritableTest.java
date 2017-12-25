package distence;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ShipLocationWritableTest implements Writable {
    private Text ship_id;
    private DoubleWritable longtitude;
    private DoubleWritable latitude;

    public ShipLocationWritableTest(String ship_id, Double longtitude, Double latitude) {
        this.ship_id = new Text(ship_id);
        this.longtitude = new DoubleWritable(longtitude);
        this.latitude = new DoubleWritable(latitude);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ship_id.readFields(in);
        longtitude.readFields(in);
        latitude.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        ship_id.write(out);
        longtitude.write(out);
        latitude.write(out);
    }
}
