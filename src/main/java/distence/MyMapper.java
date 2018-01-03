package distence;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * To get (time, ShipLocationWritable)
 * time = timestamp // TIME_UNIT 整数除法
 */
public class MyMapper extends Mapper<LongWritable, Text, LongWritable, ShipLocationWritable> {
    private static final Long TIME_UNIT = (long) 30 * 60;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        List<Double> data = Arrays.stream(value.toString().split("\\s+"))
                .map(x -> Double.parseDouble(new BigDecimal(x).toPlainString()))
                .collect(Collectors.toList());

        Long time = Math.round(data.get(0)) / TIME_UNIT;

        context.write(new LongWritable(time), new ShipLocationWritable(Math.round(data.get(3)), data.get(5), data.get(4)));
    }
}
