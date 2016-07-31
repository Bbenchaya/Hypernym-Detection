import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by asafchelouche on 31/7/16.
 */
public class HDFSTests {

    public static void main(String[] args) throws IOException {
        Path hdfsPath = new Path("test.txt");
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream out = hdfs.create(hdfsPath);
        byte[] buf = {'h', 'e', 'l', 'l', 'o', '\n'};
        out.write(buf, 0, 6);
    }

}
