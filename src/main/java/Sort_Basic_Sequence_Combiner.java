import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Sort_Basic_Sequence_Combiner {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "Basic_Sort");
        job.setJarByClass(Sort_Basic_Sequence_Combiner.class);

        job.setMapperClass(Sort_Basic_Sequence_Combiner.mapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(Reducer.class);
        job.setPartitionerClass(Sort_Basic_Sequence_Combiner.WordPartitioner.class);

        job.setNumReduceTasks(5);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class mapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        protected void map(IntWritable key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class WordPartitioner extends Partitioner<IntWritable, IntWritable> {

        @Override
        public int getPartition(IntWritable text, IntWritable IntWritable, int i) {
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