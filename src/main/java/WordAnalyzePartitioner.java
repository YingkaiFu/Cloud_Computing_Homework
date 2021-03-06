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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordAnalyzePartitioner {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "max_min_job");
        job.setJarByClass(WordAnalyze.class);

        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setNumReduceTasks(3);

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
        Text file = new Text();

        //获取文件名
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fs = (FileSplit) context.getInputSplit();
            String k = fs.getPath().getName();
            file.set(k);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.trim().length() > 0) {
                context.write(file, new LongWritable(Long.valueOf(line.trim())));
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
            context.write(new Text(key.toString() + "-sum"), new LongWritable(total));
            context.write(new Text(key.toString() + "-max"), new LongWritable(maxValue));
            context.write(new Text(key.toString() + "-min"), new LongWritable(minValue));
            context.write(new Text(key.toString() + "-avg"), new LongWritable(average));

        }

    }

    public static class WordPartitioner extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text text, LongWritable longWritable, int i) {
            int location = (int) text.toString().charAt(4);
            return location % i;
        }
    }
}
