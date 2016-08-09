import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Created by asafchelouche on 26/7/16.
 */

public class Phase2 {

    public static class Mapper2 extends Mapper<LongWritable, Text, Text, WritableLongPair> {

        private WritableLongPair count;
        private File pathsListCopy;

        /**
         * Setup a Mapper node. Copies the list of dependency paths from the S3 bucket to the local file system.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            count = new WritableLongPair(0, 1);
            boolean local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
            BufferedReader br;
            if (local) {
                br = new BufferedReader(new FileReader("resource/paths.txt"));
            } else {
                AmazonS3 s3 = new AmazonS3Client();
                Region usEast1 = Region.getRegion(Regions.US_EAST_1);
                s3.setRegion(usEast1);
                S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment3benasaf", "resource/paths.txt"));
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            }
            java.nio.file.Path path = Paths.get("resource");
            if (!Files.exists(path))
                Files.createDirectory(path);
            pathsListCopy = new File("resource/pathsListCopy.txt");
            BufferedWriter bw = new BufferedWriter(new FileWriter(pathsListCopy));
            String line;
            while ((line = br.readLine()) != null) {
                bw.write(line + "\n");
            }
            bw.close();
            br.close();
        }

        /**
         * Checks for a dependency path's index the dependency paths file. Write to context the noun pair, the dependency
         * path's index, and a count of 1.
         * @param key a noun pair.
         * @param value a dependency path.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            long index = 0;
            boolean found = false;
            String[] parts = value.toString().split("\\s");
            Scanner scanner = new Scanner(pathsListCopy);
            while (scanner.hasNextLine()) {
                if (parts[1].equals(scanner.nextLine())) {
                    found = true;
                    break;
                }
                else
                    index++;
            }
            if (found) {
                count.setL1(index);
                context.write(new Text(parts[0]), count);
            }
            scanner.close();
        }

    }

    public static class Reducer2 extends Reducer<Text, WritableLongPair, Text, Text> {

        private HashMap<String, Boolean> testSet;
        private final String BUCKET = "dsps162assignment3benasaf";
        private final String HYPERNYM_LIST = "resource/hypernym.txt";
        private final String NUM_OF_FEATURES_FILE = "resource/numOfFeatures.txt";
        private long numOfFeatures;
        private Stemmer stemmer;
        private AmazonS3 s3;

        /**
         * Setup a Reducer node.
         * @param context the Map-Reduce job context.
         * @throws IOException
         */
        @Override
        public void setup(Context context) throws IOException {
            stemmer = new Stemmer();
            boolean local = context.getConfiguration().get("LOCAL_OR_EMR").equals("true");
            Scanner scanner;
            BufferedReader br;
            if (local) {
                scanner = new Scanner(new FileReader(NUM_OF_FEATURES_FILE));
                br = new BufferedReader(new FileReader(HYPERNYM_LIST));
            } else {
                s3 = new AmazonS3Client();
                Region usEast1 = Region.getRegion(Regions.US_EAST_1);
                s3.setRegion(usEast1);
                System.out.print("Downloading no. of features file from S3... ");
                S3Object object = s3.getObject(new GetObjectRequest("dsps162assignment3benasaf", NUM_OF_FEATURES_FILE));
                System.out.println("Done.");
                scanner = new Scanner(new InputStreamReader(object.getObjectContent()));
                object = s3.getObject(new GetObjectRequest(BUCKET, HYPERNYM_LIST));
                br = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            }
            numOfFeatures = scanner.nextInt();
            System.out.println("Number of features: " + numOfFeatures);
            scanner.close();
            testSet = new HashMap<>();
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] pieces = line.split("\\s");
                stemmer.add(pieces[0].toCharArray(), pieces[0].length());
                stemmer.stem();
                pieces[0] = stemmer.toString();
                stemmer.add(pieces[1].toCharArray(), pieces[1].length());
                stemmer.stem();
                pieces[1] = stemmer.toString();
                testSet.put(pieces[0] + "$" + pieces[1], pieces[2].equals("True"));
            }
            br.close();
        }

        /**
         *
         * @param key a noun pair.
         * @param counts a list of WritableLongPair, each one being an index of a dependency path and a count of its
         *               appearances, respectively.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<WritableLongPair> counts, Context context) throws IOException, InterruptedException {
            String keyAsString = key.toString();
            keyAsString = keyAsString.substring(0, keyAsString.indexOf("#"));
            if (testSet.containsKey(keyAsString)) {
                long[] featuresVector = new long[(int) numOfFeatures];
                for (WritableLongPair count : counts) {
                    featuresVector[(int) count.getL1()] += count.getL2();
                }
                StringBuilder sb = new StringBuilder();
                for (long index : featuresVector)
                    sb.append(index).append(",");
                sb.append(testSet.get(keyAsString));
                context.write(key, new Text(sb.toString()));
            }
        }

    }

    /**
     * Main method for this Map-Reduce step. Processes the noun pairs and their dependency paths into a file which
     * contains the pairs and their features vector. This file would afterwards be processed with PostProcessor.java
     * into an .arff file for use by WEKA.
     * @param args an array of 2 Strings: input path, output path.
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IOException("Phase 2: supply 3 arguments");
        Configuration conf = new Configuration();
        conf.set("LOCAL_OR_EMR", String.valueOf(args[2].equals("local")));
        Job job = Job.getInstance(conf, "Phase 2");
        job.setJarByClass(Phase2.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(Reducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WritableLongPair.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println("Phase 2 - input path: " + args[0] + ", output path: " + args[1]);
        if (job.waitForCompletion(true))
            System.out.println("Phase 2: job completed successfully");
        else
            System.out.println("Phase 2: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Number of key-value pairs sent to reducers in phase 2: " + counter.getValue());
    }

}
