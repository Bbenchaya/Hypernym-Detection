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

    private static final String PREFIX = "@RELATION nounpair\n\n@ATTRIBUTE w1 STRING\n@ATTRIBUTE w2 STRING\n";
    private static final String POSTFIX = "@ATTRIBUTE ans {true, false}\n\n@DATA\n";
    private static final String BUCKET_NAME = "dsps162assignment3benasaf";

    public static void main(String[] args) throws IOException {
        if (!args[0].equals("local") && !args[0].equals("emr"))
            System.err.println("Usage: java PostProcessor <DPmin> [local | emr]");
        boolean local = args[0].equals("local");
        BufferedReader br1, br2;
        BufferedWriter bw1, bw2;
        if (local) {
            br1 = new BufferedReader(new FileReader("output/part-r-00000"));
            br2 = new BufferedReader(new FileReader("resource/numOfFeatures.txt"));
        } else {
            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            S3Object object1 = s3.getObject(new GetObjectRequest(BUCKET_NAME, "output_single_corpus/part-r-00000"));
            br1 = new BufferedReader(new InputStreamReader(object1.getObjectContent()));
            S3Object object2 = s3.getObject(new GetObjectRequest(BUCKET_NAME, "results/numOfFeatures.txt"));
            br2 = new BufferedReader(new InputStreamReader(object2.getObjectContent()));
        }
        bw1 = new BufferedWriter(new FileWriter("ml-input/processed_single_corpus.arff"));
        bw2 = new BufferedWriter(new FileWriter("ml-input/processed_single_corpus_with_words.arff"));
        int vectorLength = Integer.parseInt(br2.readLine());
        System.out.println("Number of features: " + vectorLength);
        String line;
        bw1.write(PREFIX);
        bw2.write(PREFIX);
        for (int i = 0; i < vectorLength; i++) {
            bw1.write("@ATTRIBUTE p" + i + " REAL\n");
            bw2.write("@ATTRIBUTE p" + i + " REAL\n");
        }
        bw1.write(POSTFIX);
        bw2.write(POSTFIX);
        while ((line = br1.readLine()) != null) {
            bw1.write(line.substring(0, line.indexOf("#")).replaceAll("[$]", ",") + ",");
            bw1.write(line.substring(line.indexOf("\t") + 1) + "\n");
            bw2.write(line.substring(0, line.indexOf("\t")).replaceAll("[$]", ",") + ",");
            bw2.write(line.substring(line.indexOf("\t") + 1) + "\n");
        }
        br1.close();
        br2.close();
        bw1.close();
        bw2.close();
    }

}
