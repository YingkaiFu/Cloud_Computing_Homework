import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class SearchWithKey {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        String keyName = args[1];
        SequenceFile.Reader reader = null;
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream out = hdfs.create(new Path(args[2]));
        try {
            SequenceFile.Reader.Option optionfile = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(conf, optionfile);

            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                //判断键是否等于输入的键名
                if (key.toString().equals(keyName)) {
                    //如果相等，则输出信息到控制台中，同时写入文件
                    String syncSeen = reader.syncSeen() ? "*" : "";
                    System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
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
