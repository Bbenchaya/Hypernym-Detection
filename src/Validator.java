import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.*;
import java.util.Scanner;

/**
 * Created by asafchelouche on 31/7/16.
 */
public class Validator {

    public static void main(String[] args) throws IOException {
        Scanner sc = new Scanner(new FileReader("output/part-r-00000"));
        int f = 0;
        int t = 0;
        String line;
        while (sc.hasNextLine()) {
            line = sc.nextLine();
            if (line.contains("true"))
                t++;
            else if (line.contains("false"))
                f++;
        }
        System.out.format("True: %d, False: %d\n", t, f);
    }
}
