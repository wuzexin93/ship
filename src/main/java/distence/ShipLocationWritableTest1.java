package distence;

import distence.interfaces.ShipLocationInterface;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ShipLocationWritableTest1 implements Writable, ShipLocationInterface {
    private long ship_id;
    private String longtitude;
    private String latitude;

    public ShipLocationWritableTest1(long ship_id, String longtitude, String latitude) {
        this.ship_id = ship_id;
        this.longtitude = longtitude;
        this.latitude = latitude;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        String[] fields = in.readLine().split(",");
        ship_id = Long.parseLong(fields[0]);
        longtitude = fields[1];
        latitude = fields[2];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeBytes(ship_id + "," + longtitude + "," + latitude);
    }

    @Override
    public Double getDistenceWith(ShipLocationInterface shipLocation) {
        return null;
    }
}
