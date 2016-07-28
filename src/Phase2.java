import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by asafchelouche on 26/7/16.
 */
public class Phase2 {

    static String pathsListFilename;
    private static FileSystem hdfs;

    static class Mapper2 extends Mapper<LongWritable, Text, Text, WritableLongPair> {

        private BufferedReader br;
        private WritableLongPair count;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            count = new WritableLongPair(0, 1);
            br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(pathsListFilename))));
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            long index = 0;
            boolean found = false;
            String line;
            String[] parts = value.toString().split("\\s");
            while ((line = br.readLine()) != null) {
                if (parts[1].equals(line)) {
                    found = true;
                    break;
                }
                else
                    index++;
            }
            if (found) {
                count.setL1(index);
                context.write(new Text(parts[1]), count);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            br.close();
        }

    }

    static class Combiner2 extends Reducer<Text, WritableLongPair, Text, WritableLongPair> {

        @Override
        public void setup(Context context) throws IOException {

        }

        @Override
        public void reduce(Text key, Iterable<WritableLongPair> counts, Context context) throws IOException, InterruptedException {

        }

        @Override
        public void cleanup(Context context) {

        }

    }

    static class Reducer2 extends Reducer<Text, WritableLongPair, Text, Text> {

        private HashMap<String, Boolean> testSet;
        private final String BUCKET = "dsps162assignment3benasaf";
        private final String HYPERNYM_LIST = "hypernym.txt";
        private long numOfFeatures;

        @Override
        public void setup(Context context) throws IOException {
            numOfFeatures = Phase1.numOfFeatures;
            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            S3Object object = s3.getObject(new GetObjectRequest(BUCKET, HYPERNYM_LIST));
            testSet = new HashMap<>();
            BufferedReader br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] pieces = line.split("\\s");
                testSet.put(pieces[0] + "$" + pieces[1], pieces[2].equals("True"));
            }
            br.close();
        }

        @Override
        public void reduce(Text key, Iterable<WritableLongPair> counts, Context context) throws IOException, InterruptedException {

        }

        @Override
        public void cleanup(Context context) {

        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IOException("Phase 2: supply 3 arguments");
        pathsListFilename = args[2];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 2");
        job.setJarByClass(Phase2.class);
        job.setMapperClass(Mapper2.class);
//        job.setCombinerClass(Combiner2.class);
        job.setReducerClass(Reducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WritableLongPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("Phase 2 - input path: " + args[0] + ", output path: " + args[1]);
        hdfs = FileSystem.get(conf);
        if (job.waitForCompletion(true))
            System.out.println("Phase 2: job completed successfully");
        else
            System.out.println("Phase 2: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 2: " + counter.getValue());
//        AmazonS3 s3 = new AmazonS3Client();
//        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
//        s3.setRegion(usEast1);
//        try {
//            System.out.print("Uploading the corpus description file to S3... ");
//            File file = new File(WORDS_PER_DECADE_FILENAME);
//            FileWriter fw = new FileWriter(file);
//            for (int i = 0; i < NUM_OF_DECADES; i++)
//                fw.write(Long.toString(job.getCounters().findCounter("Phase1$Mapper1$CountersEnum", "DECADE_" + i).getValue()) + "\n");
//            fw.flush();
//            fw.close();
//            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", WORDS_PER_DECADE_FILENAME, file));
//            System.out.println("Done.");
//            System.out.print("Uploading Phase 1 description file to S3... ");
//            file = new File(NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME);
//            fw = new FileWriter(file);
//            fw.write(Long.toString(counter.getValue()) + "\n");
//            fw.flush();
//            fw.close();
//            s3.putObject(new PutObjectRequest("dsps162assignment2benasaf/results/", NUM_OF_PAIRS_SENT_TO_REDUCERS_FILENAME, file));
//            System.out.println("Done.");
//        } catch (AmazonServiceException ase) {
//            System.out.println("Caught an AmazonServiceException, which means your request made it "
//                    + "to Amazon S3, but was rejected with an error response for some reason.");
//            System.out.println("Error Message:    " + ase.getMessage());
//            System.out.println("HTTP Status Code: " + ase.getStatusCode());
//            System.out.println("AWS Error Code:   " + ase.getErrorCode());
//            System.out.println("Error Type:       " + ase.getErrorType());
//            System.out.println("Request ID:       " + ase.getRequestId());
//        } catch (AmazonClientException ace) {
//            System.out.println("Caught an AmazonClientException, which means the client encountered "
//                    + "a serious internal problem while trying to communicate with S3, "
//                    + "such as not being able to access the network.");
//            System.out.println("Error Message: " + ace.getMessage());
//        }
    }

}
