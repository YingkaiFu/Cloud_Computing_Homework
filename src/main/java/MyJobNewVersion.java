import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class MyJobNewVersion extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new MyJobNewVersion(), args);

        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("key.value.separator.in.input.line", ",");

        /*
         * 统一使用Job类来管理Mapreduce作业
         *
         * 老版的使用JobConf来管理
         */
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "MyJob");
        job.setJarByClass(MyJobNewVersion.class);

        Path in = new Path(args[0]);
        Path out = new Path(args[1]);
        FileInputFormat.setInputPaths(job, in);
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



        /*
         * 提交job的编号
         *
         * 老版本
         * JobClient.runJob(job);
         */
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        return 0;
    }

    public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] citation = value.toString().split(",");

            /*
             * 老版本
             * output.collect
             */
            context.write(new Text(citation[1]), new Text(citation[0]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String csv = "";
            for (Text val : values) {
                if (csv.length() > 0) csv += ",";
                csv += val.toString();
            }

            /*
             * 老版本
             * output.collect
             */
            context.write(key, new Text(csv));
        }
    }
}
