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
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

public class Sort_Basic_Partitional {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "Basic_Sort Partition");
        job.setJarByClass(Sort_Basic_Partitional.class);

        job.setMapperClass(Sort_Basic_Partitional.mapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(Sort_Basic_Partitional.reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(5);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.getLocal(configuration);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path(args[2]));
        InputSampler.Sampler<LongWritable, LongWritable> sampler = new InputSampler.RandomSampler<LongWritable, LongWritable>(0.1, 10000, 5);
        InputSampler.writePartitionFile(job, sampler);
        job.setPartitionerClass(TotalOrderPartitioner.class);

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

}