import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by asafchelouche on 26/7/16.
 */

public class Phase1 {

    private static int DPmin;
    private static String pathsFilename;

    static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        private Stemmer stemmer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new Stemmer();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value);

            // construct tree from ngram
            // stem words
            // for each dep-path, emit shit
        }

        @Override
        public void cleanup(Context context) {

        }

    }

    static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        private Counter featureCounter;
        enum CountersEnum {NUM_OF_FEATURES}
        private File pathsFile;
        private BufferedWriter bw;

        @Override
        public void setup(Context context) throws IOException {
            featureCounter = context.getCounter(CountersEnum.class.getName(), CountersEnum.NUM_OF_FEATURES.toString());
            pathsFile = new File("pathsFileHDFSPath.txt");
            bw = new BufferedWriter(new FileWriter(pathsFile));
        }

        @Override
        public void reduce(Text key, Iterable<Text> counts, Context context) throws IOException, InterruptedException {

        }

        @Override
        public void cleanup(Context context) {
            // TODO write paths-file to HDFS
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IOException("Phase 1: supply 4 arguments");
        DPmin = Integer.parseInt(args[2]);
        pathsFilename = args[3];
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Phase 1");
        job.setJarByClass(Phase1.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("Phase 1 - input path: " + args[0] + ", output path: " + args[1]);
        if (job.waitForCompletion(true))
            System.out.println("Phase 1: job completed successfully");
        else
            System.out.println("Phase 1: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 1: " + counter.getValue());
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
