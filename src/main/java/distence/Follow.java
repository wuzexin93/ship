package distence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Follow {
    public static void main(String[] args) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(args);

        String[] remainingArgs = parser.getRemainingArgs();
        Configuration configuration = parser.getConfiguration();
        Job job = Job.getInstance(configuration, "Find follow");

        job.setJarByClass(Follow.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(ShipLocationWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(PairWritable.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
