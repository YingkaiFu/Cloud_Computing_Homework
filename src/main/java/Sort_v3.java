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

public class Sort_v3 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "Secondly_Sort");
        job.setJarByClass(Sort_v3.class);

        job.setMapperClass(Sort_v3.mapper.class);
        job.setReducerClass(Sort_v3.reducer.class);

        job.setMapOutputKeyClass(DoubleData.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(DoubleData.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(DoubleGroupingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path path = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FileOutputFormat.setOutputPath(job, path);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


    public static class mapper extends Mapper<LongWritable, Text, DoubleData, LongWritable> {

        private DoubleData doubleData = new DoubleData();
        private LongWritable v = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(",");
            doubleData.setInt1(new LongWritable(Long.valueOf(words[0].trim())));
            doubleData.setInt2(new LongWritable(Long.valueOf(words[1].trim())));
            v.set(Long.valueOf(words[0].trim()));
            context.write(doubleData, v);
        }
    }

    public static class reducer extends Reducer<DoubleData, LongWritable, LongWritable, Text> {
        StringBuilder sb = new StringBuilder();
        Text k = new Text();
        @Override
        protected void reduce(DoubleData key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            sb.delete(0, sb.length());
            for (LongWritable value : values) {
                sb.append(value.toString());
                sb.append(",");
            }
            if (sb.length() > 0) {
                sb.deleteCharAt(sb.length() - 1);
            }
            k.set(sb.toString());
            context.write(key.getInt2(), k);
        }
    }
}
