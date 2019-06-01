import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;


public class SearchWithFileAndKey {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(args[0]);
        String fileName = args[1];
        String keyName = args[2];
        SequenceFile.Reader reader = null;
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream out = hdfs.create(new Path(args[3]));
        try {
            SequenceFile.Reader.Option optionfile = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(conf, optionfile);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String s = value.toString().split("#")[2];
                //判断文件名和键是否和输入的相等
                if (s.equals(fileName) && key.toString().equals(keyName)) {
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
