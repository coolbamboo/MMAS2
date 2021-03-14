package hdfs_to_local;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * a tool to download result files from hdfs to local
 */
public class HdfsUtil {
    public static void main(String args[]) throws URISyntaxException, IOException {
        String hdfs_host = "hdfs://192.168.30.50:9000/";
        String hdfs_dir = "hdfs://192.168.30.50:9000/WTA/data/output/";
        Path path = new Path(hdfs_dir + "part-00000");

        FileSystem fs = FileSystem.get(new URI(hdfs_host), new Configuration());
        if (fs.exists(path)) {
            InputStream in = fs.open(path);
            //append local
            OutputStream out = new FileOutputStream("./results.csv", true);
            IOUtils.copyBytes(in, out, 4096, true);
            //delete dir
            fs.deleteOnExit(new Path(hdfs_dir));
        }
        fs.close();
    }
}
