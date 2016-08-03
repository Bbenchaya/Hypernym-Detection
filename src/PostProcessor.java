import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.*;

/**
 * Created by asafchelouche on 31/7/16.
 */
public class PostProcessor {

    private static final String PREFIX = "@RELATION nounpair\n\n";
    private static final String POSTFIX = "@ATTRIBUTE ans {true, false}\n\n@DATA\n";

    public static void main(String[] args) throws IOException {
        int vectorLength = Integer.parseInt(args[0]);
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment3benasaf/output_single_corpus", "part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
        BufferedWriter bw = new BufferedWriter(new FileWriter("processed_single_corpus.arff"));
        String line;
        bw.write(PREFIX);
        for (int i = 0; i < vectorLength; i++)
            bw.write("@ATTRIBUTE p" + i + " REAL\n");
        bw.write(POSTFIX);
        while ((line = br.readLine()) != null)
            bw.write(line.substring(line.indexOf("\t") + 1) + "\n");
        br.close();
        bw.close();
    }

}
