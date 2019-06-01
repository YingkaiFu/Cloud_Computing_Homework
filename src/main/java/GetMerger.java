import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;

public class GetMerger {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        String uri = "hdfs://hwi:9000";
        Path inputDir = new Path(uri + args[0]);
        Path outFile = new Path(args[1]);
        try {
            FileSystem hdfs = FileSystem.get(new URI(uri), conf);
            FileSystem local = FileSystem.getLocal(conf);

            FileStatus[] inputFiles = hdfs.listStatus(inputDir);
            FSDataOutputStream out = local.create(outFile);

            for (int i = 0; i < inputFiles.length; i++) {
                System.out.println(inputFiles[i].getPath().getName());
                FSDataInputStream in = hdfs.open(inputFiles[i].getPath());
                byte buffer[] = new byte[256];
                int bytesRead = 0;
                while ((bytesRead = in.read(buffer)) > 0) {
                    out.write(buffer, 0, bytesRead);
                }
                in.close();
            }
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
