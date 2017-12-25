package distence;

import distence.interfaces.PairInterface;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

public class PairWritable implements Writable, PairInterface {
    private double ship_1;
    private double ship_2;

    public PairWritable(double ship_1, double ship_2) {
        this.ship_1 = ship_1;
        this.ship_2 = ship_2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ship_1 = in.readDouble();
        ship_2 = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(ship_1);
        out.writeDouble(ship_2);
    }

    public double getShip_1() {
        return ship_1;
    }

    public double getShip_2() {
        return ship_2;
    }

    /**
     * Judge whether it is the same pair
     * @param pair another pair instance
     * @return equal or not
     */
    public boolean equalTo(PairWritable pair) {
        HashSet<Double> set1 = new HashSet<>();
        set1.add(ship_1);
        set1.add(ship_2);

        HashSet<Double> set2 = new HashSet<>();
        set2.add(pair.getShip_1());
        set2.add(pair.getShip_2());

        return set1.equals(set2);
    }
}
