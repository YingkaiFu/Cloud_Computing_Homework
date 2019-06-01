
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;


public class MRPreDefined {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "word count");
        job.setJarByClass(MRPreDefined.class);


        FileInputFormat.setInputPaths(job, new Path("testdata/input3"));
        FileOutputFormat.setOutputPath(job, new Path("testdata/output0514/4"));

        //test1 直接输出
//        job.setOutputKeyClass(LongWritable.class);	//输出Key的数据类型
//        job.setOutputValueClass(Text.class);			//输出Value的数据类型
//        job.setMapperClass(Mapper.class);				//预定义
//        job.setReducerClass(Reducer.class);			//预定义


        //test2 逆转输出
//        job.setOutputKeyClass(Text.class);             //输出Key的数据类型
//        job.setOutputValueClass(LongWritable.class);   //输出Value的数据类型
//        job.setMapperClass(InverseMapper.class);    //预定义
//        job.setReducerClass(Reducer.class);    //预定义


        //test3 求和输出
        job.setOutputKeyClass(Text.class);             //输出Key的数据类型
        job.setOutputValueClass(IntWritable.class);   //输出Value的数据类型
        job.setMapperClass(TokenCounterMapper.class);    //预定义
        job.setReducerClass(IntSumReducer.class);    //预定义


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}





