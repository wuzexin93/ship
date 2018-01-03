package distence;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class MyReducer extends Reducer<LongWritable, ShipLocationWritable, LongWritable, PairWritable> {
    private static final double LIMIT = 1; // 单位是海里，但是求距离的单位还不知道，需要测试，暂且不管

    @Override
    protected void reduce(LongWritable key, Iterable<ShipLocationWritable> values, Context context) throws IOException, InterruptedException {
        Map<Long, ShipLocationWritable> writablesMap = new HashMap<>();
        Map<Long, Integer> countsMap = new HashMap<>();

        for (ShipLocationWritable value : values) { //同一时间，同一艘船，仅出现一次，先求和，再求平均
            Long ship_id = value.getShipId();

            countsMap.put(ship_id, countsMap.getOrDefault(ship_id, 0) + 1);
            writablesMap.put(ship_id, writablesMap.getOrDefault(ship_id, ShipLocationWritable.NULL_SHIP).locationSum(value));
        }

        for (Map.Entry entry : countsMap.entrySet()) {
            Long ship_id = (Long) entry.getKey();
            Integer count = (Integer) entry.getValue();
            if (count > 1) {
                writablesMap.put(ship_id, new ShipLocationWritable(ship_id,
                        writablesMap.get(ship_id).getLongtitude() / count, writablesMap.get(ship_id).getLatitude() /count));
            }
        }

        HashSet<PairWritable> results = findPair(writablesMap);
        if (results.size() > 0) {
            for (PairWritable writable : results)
                context.write(key, writable); //不知道写在文件里啥样子
        }
    }

    private static HashSet<PairWritable> findPair(Map<Long, ShipLocationWritable> map) {
        int size = map.size();
        ShipLocationWritable[] writables = new ShipLocationWritable[size]; //可能以后需要处理大数据，数组格式可能不合适
        map.values().toArray(writables);
        HashSet<PairWritable> results = new HashSet<>();

        if (size > 1) {
            for (int i = 0; i < size - 1; i++) {
                ShipLocationWritable ship1 = writables[i];
                for (int j = i + 1; j < size; j++) {
                    Double distence = ship1.getDistenceWith(writables[j]);

                    if (distence < LIMIT) {
                        results.add(new PairWritable(ship1.getShipId(), writables[j].getShipId()));
                    }
                }
            }
        }

        return results;
    }
}
