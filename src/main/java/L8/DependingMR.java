package L8;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DependingMR {

	private static final Text TEXT_SUM = new Text("SUM");
	private static final Text TEXT_COUNT = new Text("COUNT");
	private static final Text TEXT_AVG = new Text("AVG");

	// ����SumMapper���൥��Map�е��������
	public static class SumMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		public long sum = 0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			sum += Long.parseLong(value.toString());
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(TEXT_SUM, new LongWritable(sum));
		}
	}

	// ����SumReducer�����
	public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		public long sum = 0;

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			for (LongWritable v : values) {
				sum += v.get();
			}
			context.write(TEXT_SUM, new LongWritable(sum));
		}
	}

	// ����CountMapper��ͳ������
	public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		public long count = 0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			count += 1;
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(TEXT_COUNT, new LongWritable(count));
		}
	}

	// ����CountReducer��ͳ������
	public static class CountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		public long count = 0;

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			for (LongWritable v : values) {
				count += v.get();
			}
			context.write(TEXT_COUNT, new LongWritable(count));
		}
	}

	
	// ����ƽ����MR��Map
	public static class AvgMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

		public long count = 0;
		public long sum = 0;

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("\t");
			if (v[0].equals("COUNT")) {
				count = Long.parseLong(v[1]);
			} else if (v[0].equals("SUM")) {
				sum = Long.parseLong(v[1]);
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(sum), new LongWritable(count));
		}

	}
	
	// ����ƽ����MR��Reduce
	public static class AvgReducer extends Reducer<LongWritable, LongWritable, Text, DoubleWritable> {

		public long sum = 0;
		public long count = 0;

		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			sum += key.get();
			for (LongWritable v : values) {
				count += v.get();
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(TEXT_AVG, new DoubleWritable(new Double(sum) / count));
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		String inputPath = "testdata/lab4";
		String sumOutputPath = "testdata/lab4-out/sum";
		String countOutputPath = "testdata/lab4-out/count";
		String avgOutputPath = "testdata/lab4-out/avg";
		
		FileSystem fs=FileSystem.getLocal(conf);
		if(fs.exists(new Path(sumOutputPath))) fs.delete(new Path("testdata/lab4-out/"),true);		

		Job job1 = Job.getInstance(conf, "Sum");
		job1.setJarByClass(DependingMR.class);
		job1.setMapperClass(SumMapper.class);
		job1.setCombinerClass(SumReducer.class);
		job1.setReducerClass(SumReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(LongWritable.class);		
		FileInputFormat.addInputPath(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(sumOutputPath));

		Job job2 = Job.getInstance(conf, "Count");
		job2.setJarByClass(DependingMR.class);
		job2.setMapperClass(CountMapper.class);
		job2.setCombinerClass(CountReducer.class);
		job2.setReducerClass(CountReducer.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(countOutputPath));

		Job job3 = Job.getInstance(conf, "Average");
		job3.setJarByClass(DependingMR.class);
		job3.setMapperClass(AvgMapper.class);
		job3.setReducerClass(AvgReducer.class);
		job3.setMapOutputKeyClass(LongWritable.class);
		job3.setMapOutputValueClass(LongWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);

		// ��job1��job2�����Ϊ��job3������
		FileInputFormat.addInputPath(job3, new Path(sumOutputPath));
		FileInputFormat.addInputPath(job3, new Path(countOutputPath));
		FileOutputFormat.setOutputPath(job3, new Path(avgOutputPath));
		
		ControlledJob contlJob1=new ControlledJob(job1.getConfiguration());
		contlJob1.setJob(job1);
		
		ControlledJob contlJob2=new ControlledJob(job2.getConfiguration());
		contlJob2.setJob(job2);
		
		ControlledJob contlJob3=new ControlledJob(job3.getConfiguration());
		contlJob3.setJob(job3);
		contlJob3.addDependingJob(contlJob1);
		contlJob3.addDependingJob(contlJob2);
		
		JobControl jobContl=new JobControl("depending job");
		jobContl.addJob(contlJob1);
		jobContl.addJob(contlJob2);
		jobContl.addJob(contlJob3);
		
		Thread t=new Thread(jobContl);
		t.start();
		
		while (true) {
			if (jobContl.allFinished()) {
				System.out.println(jobContl.getSuccessfulJobList());
				jobContl.stop();
				break;
			}
			if (jobContl.getFailedJobList().size() > 0) {
				System.out.println(jobContl.getFailedJobList());
				jobContl.stop();
				break;
			}
		}    
	}
}
