package distence;

import distence.interfaces.ShipLocationInterface;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class ShipLocationWritable implements Writable, ShipLocationInterface {
    private long ship_id;
    private double longtitude;
    private double latitude;

    /**
     *
     * @param ship_id 数值类型
     * @param longtitude 经度，规定：东经0-180，西经0W：181-360
     * @param latitude 纬度，规定：北纬0-180，南纬0- -180
     */
    public ShipLocationWritable(long ship_id, double longtitude, double latitude) {
        this.ship_id = ship_id;
        this.longtitude = longtitude;
        this.latitude = latitude;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ship_id = in.readLong();
        longtitude = in.readDouble();
        latitude = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(ship_id);
        out.writeDouble(longtitude);
        out.writeDouble(latitude);
    }

    public double getLatitude() {
        return latitude;
    }

    public long getShip_id() {
        return ship_id;
    }

    public double getLongtitude() {
        return longtitude;
    }

    /**
     * 同一时间段内，同一艘船有多个数据点，用所有点的平均值代替？
     *
     * @param shipLocation
     * @return
     */

    public Double getDistenceWith(ShipLocationWritable shipLocation) {
        return LocationUtils.getDistance(latitude, longtitude, shipLocation.latitude, shipLocation.longtitude);
    }
}
