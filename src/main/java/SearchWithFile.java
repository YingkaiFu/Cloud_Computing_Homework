import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;


public class SearchWithFile {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        String fileName = args[1];
        SequenceFile.Reader reader = null;
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream out = hdfs.create(new Path(args[2]));
        try {
            //使用了新版的Reader定义方式
            SequenceFile.Reader.Option optionfile = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(conf, optionfile);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                //由于使用了两个#，因此我们选取第二个#之后的数据，其表示文件名
                String s = value.toString().split("#")[2];
                //判断是否和输入的文件名相同
                if (s.equals(fileName)) {
                    //如果文件名相同，则将其写入流中输出
                    out.writeChars(key + "\t" + value + "\n");
                }
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
            IOUtils.closeStream(out);
        }
    }
}
