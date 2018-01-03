package distence;

import distence.interfaces.PairInterface;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

public class PairWritable implements Writable, PairInterface {
    private long ship_1;
    private long ship_2;

    public PairWritable(long ship_1, long ship_2) {
        this.ship_1 = ship_1;
        this.ship_2 = ship_2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ship_1 = in.readLong();
        ship_2 = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(ship_1);
        out.writeLong(ship_2);
    }

    public long getShip_1() {
        return ship_1;
    }

    public long getShip_2() {
        return ship_2;
    }

    /**
     * Judge whether it is the same pair
     * @param pair another pair instance
     * @return equal or not
     */
    public boolean equalTo(PairWritable pair) {
        HashSet<Long> set1 = new HashSet<>();
        set1.add(ship_1);
        set1.add(ship_2);

        HashSet<Long> set2 = new HashSet<>();
        set2.add(pair.getShip_1());
        set2.add(pair.getShip_2());

        return set1.equals(set2);
    }

    @Override
    public boolean equals(Object obj) {
        return this.equalTo((PairWritable) obj);
    }

    @Override
    public String toString() {
        return "(" + ship_1 + "," + ship_2 + ")";
    }

    /**
     * (a,b)与(b,a)不同，用于HashSet区别不同元素
     * 目前可能出现哈希冲突
     * @return
     */
    @Override
    public int hashCode() {
        return (int) ship_1 << 2 & (int) ship_2 << 2;
    }
}
