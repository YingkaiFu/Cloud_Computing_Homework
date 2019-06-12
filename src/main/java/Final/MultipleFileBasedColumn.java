package Final;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * MultipleOutputsʵ�ֶ����ļ����ļ��Ķ�ṹ���
 * 
 * @author weili wangwri
 * 2019��5��24��
 */
public class MultipleFileBasedColumn {
	
	public static class MultipleOutMapper extends 
	Mapper<LongWritable, Text, Text, Text>{
		
		private MultipleOutputs<Text, Text> mos;
		private Text city=new Text();
		private Text NameTell=new Text();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			mos=new MultipleOutputs<Text, Text>(context);
		}
		

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values=value.toString().split(",");
			city.set(values[3]);
			NameTell.set("Name:"+values[1]+", Tel:"+values[4]);
			//���1����ͳ����������ȫ��������,�ļ���Ϊpart-
			context.write(city, NameTell);  	
			
			//���2��Ϊ��������������ļ������ļ���Ϊ����ĳ���
			mos.write(city, NameTell, values[3]);	
			
			//���3��Ϊ���������ļ��У����������������
			mos.write(city, NameTell, values[3]+"/output");	
			
			//���4��������ʽ�ļ������
			mos.write("text",city, NameTell);	
			mos.write("seq",city, NameTell);	
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			mos.close();
		}		
	}	

	
		
    @SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
    	Configuration conf=new Configuration();
    	Job job = new Job(conf, "word count");
    	job.setJarByClass(MultipleFileBasedColumn.class);


        FileInputFormat.setInputPaths(job,
        		new Path("testdata/input5"));
        FileOutputFormat.setOutputPath(job, 
        		new Path("testdata/input5-out/"+System.currentTimeMillis()));
        
        MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, 
        		Text.class,Text.class);
        
        MultipleOutputs.addNamedOutput(job, "seq", SequenceFileOutputFormat.class, 
        		Text.class,Text.class);        
        
//        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(Text.class);	//���Key����������
        job.setOutputValueClass(Text.class);			//���Value����������
        job.setMapperClass(MultipleOutMapper.class);
        job.setNumReduceTasks(0);           
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
