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
        AmazonS3 s3 = new AmazonS3Client();
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3.setRegion(usEast1);
        S3Object object1 = s3.getObject(new GetObjectRequest("dsps162assignment3benasaf/output_single_corpus", "part-r-00000"));
        BufferedReader br1 = new BufferedReader(new InputStreamReader(object1.getObjectContent()));
        S3Object object2 = s3.getObject(new GetObjectRequest("dsps162assignment3benasaf/results", "numOfFeatures.txt"));
        BufferedReader br2 = new BufferedReader(new InputStreamReader(object2.getObjectContent()));
        int vectorLength = Integer.parseInt(br2.readLine());
        System.out.println("Number of features: " + vectorLength);
//        BufferedReader br1 = new BufferedReader(new FileReader("output/part-r-00000")); // If reading a local file
        BufferedWriter bw = new BufferedWriter(new FileWriter("processed_single_corpus.arff"));
        String line;
        bw.write(PREFIX);
        for (int i = 0; i < vectorLength; i++)
            bw.write("@ATTRIBUTE p" + i + " REAL\n");
        bw.write(POSTFIX);
        while ((line = br1.readLine()) != null)
            bw.write(line.substring(line.indexOf("\t") + 1) + "\n");
        br1.close();
        br2.close();
        bw.close();
    }

}
