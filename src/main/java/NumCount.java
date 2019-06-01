import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NumCount {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "num_count_job");
        job.setJarByClass(WordAnalyze.class);

        job.setMapperClass(NumCount.Map.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(NumCount.Reduce.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<Object, Text, LongWritable, Text> {
        final Text v = new Text();
        final LongWritable k = new LongWritable();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = String.valueOf(key);
            long length = s.length();
            k.set(length);
            context.write(k, value);
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> {
        final Text v = new Text();

        protected void reduce(LongWritable key, Iterable<Text> values,
                              Context context) throws IOException, InterruptedException {
            int num = 0, max = 100000;
            for (Text value : values) {
                num++;
            }
            double rate = 0.000;
            rate = num / 1000;
            v.set(String.valueOf(rate) + "%");
            context.write(key, v);
        }
    }
}
