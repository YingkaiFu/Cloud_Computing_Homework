import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordAnalyzeCombiner {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "max_min_job");
        job.setJarByClass(WordAnalyze.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static LongWritable out = new LongWritable(1);
        Text file = new Text("1");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().length() > 0) {
                context.write(new Text("textkey"), new LongWritable(Long.valueOf(line.trim())));
            }
        }
    }

    public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long maxValue = Long.MIN_VALUE;
            long minValue = Long.MAX_VALUE;
            long total = 0;
            long count = 0;
            for (LongWritable value : values) {
                maxValue = Math.max(value.get(), maxValue);
                minValue = Math.min(value.get(), minValue);
                total = total + value.get();
                count = count + 1;
            }
            long average = total / count;
            context.write(new Text("max"), new LongWritable(maxValue));
            context.write(new Text("min"), new LongWritable(minValue));
            context.write(new Text("ave"), new LongWritable(average));
            context.write(new Text("sum"), new LongWritable(total));
        }

    }
}
