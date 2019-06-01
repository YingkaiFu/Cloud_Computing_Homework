import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;

import java.io.IOException;
import java.net.URI;

public class Compress {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        String uri = "hdfs://hwi:9000";
        Path inputDir = new Path(uri + args[0]);
        Path outFile = new Path(args[1]);
        try {
            FileSystem hdfs = FileSystem.get(new URI(uri), conf);
            FileSystem local = FileSystem.getLocal(conf);
            GzipCodec gzipCodec = new GzipCodec();
            gzipCodec.setConf(conf);
            FileStatus[] inputFiles = hdfs.listStatus(inputDir);
            //创建压缩的输出流
            CompressionOutputStream compressionOutputStream =
                    gzipCodec.createOutputStream(local.create(outFile));
            //对于每一个文件，做如下处理
            for (FileStatus inputFile : inputFiles) {
                System.out.println(inputFile.getPath().getName());
                //逐个打开待压缩的文件，读取其中的内容
                FSDataInputStream in = hdfs.open(inputFile.getPath());
                //使用Hadoop的IOUtils来完成对流的拷贝
                IOUtils.copyBytes(in, compressionOutputStream, 4096, false);
                in.close();
            }
            compressionOutputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
