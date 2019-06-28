import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SequenceFileWriteFile {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws IOException, ClassNotFoundException {

        Path outFile = new Path(args[1]);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.getLocal(conf);
        IntWritable key = new IntWritable();
        IntWritable value = new IntWritable();
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, outFile, key.getClass(), value.getClass(), CompressionType.NONE);
        try {
            FileSystem local = FileSystem.getLocal(conf);
            //获取输入参数目录下的所有文件
            FileStatus[] files = local.listStatus(new Path(args[0]));
            for (FileStatus file : files) {
                FSDataInputStream in = local.open(file.getPath());
                //使用流来逐个读取文件
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String line;
                while ((line = br.readLine()) != null) {
                    //对于每一个记录，以制表符为界，前面的数据作为key，后面的作为value
                    key.set(Integer.parseInt(line.substring(0, line.indexOf(","))));
                    value.set(Integer.parseInt(line.substring(line.indexOf(",") + 1)));
                    writer.append(key, value);
                }
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

