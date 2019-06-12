import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Sort_Basic {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "Basic_Sort");
        job.setJarByClass(Sort_Basic.class);

        job.setMapperClass(Sort_Basic.mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(Sort_Basic.reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setPartitionerClass(WordPartitioner.class);
        job.setNumReduceTasks(5);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class mapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(",");
            context.write(new LongWritable(Long.valueOf(words[0].trim())), new LongWritable(Long.valueOf(words[1].trim())));
        }
    }

    public static class reducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static class WordPartitioner extends Partitioner<LongWritable, LongWritable> {

        @Override
        public int getPartition(LongWritable text, LongWritable longWritable, int i) {
            long value = text.get();
            if (value <= 20000) {
                return 0;
            } else if (value <= 40000) {
                return 1;
            } else if (value <= 60000) {
                return 2;
            } else if (value <= 80000) {
                return 3;
            } else return 4;
        }
    }
}