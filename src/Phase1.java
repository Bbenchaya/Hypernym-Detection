import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
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
import java.util.LinkedList;

/**
 * Created by asafchelouche on 26/7/16.
 */

public class Phase1 {

    private static int DPmin;

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        private Stemmer stemmer;
        private final String REGEX = "[^a-zA-Z ]+";

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new Stemmer();
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] components = value.toString().split("\t");
            String ngram = components[1];
            String[] parts = ngram.split(" ");
            Node[] nodes = getNodes(parts);
            if (nodes == null)
                return;
            Node root = constructParsedTree(nodes);
            searchDependencyPath(root, "", "", context);
        }

        private Node[] getNodes(String[] parts) {
            Node[] partsAsNodes = new Node[parts.length];
            for (int i = 0; i < parts.length; i++) {
                String[] ngramEntryComponents = parts[i].split("/");
                if (ngramEntryComponents.length != 4)
                    return null;
                ngramEntryComponents[0] = ngramEntryComponents[0].replaceAll(REGEX, "");
                if (ngramEntryComponents[0].replaceAll(REGEX, "").equals(""))
                    return null;
                ngramEntryComponents[1] = ngramEntryComponents[1].replaceAll(REGEX, "");
                if (ngramEntryComponents[1].replaceAll(REGEX, "").equals(""))
                    return null;
                partsAsNodes[i] = new Node(ngramEntryComponents, stemmer);
            }
            return partsAsNodes;
        }

        private Node constructParsedTree(Node[] nodes) {
            int rootIndex = 0;
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i].getFather() > 0)
                    nodes[nodes[i].getFather() - 1].addChild(nodes[i]);
                else
                    rootIndex = i;
            }
            return nodes[rootIndex];
        }

        private void searchDependencyPath(Node node, String acc, String firstWord, Context context) throws IOException, InterruptedException {
            if (node.isNoun() && acc.isEmpty())
                for (Node child : node.getChildren())
                    searchDependencyPath(child, node.getDepencdencyPathComponent(), node.getWord(), context);
            else if (node.isNoun()) {
                    Text a = new Text(acc + ":" + node.getDepencdencyPathComponent());
                    Text b = new Text(firstWord + "$" + node.getWord());
                    context.write(a, b);
            } else { // node isn't noun, but the accumulator isn't empty
                for (Node child : node.getChildren())
                    searchDependencyPath(child, acc.isEmpty() ? acc : acc + ":" + node.getDepencdencyPathComponent(), firstWord, context);
            }
        }

    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        private File pathsFile;
        private BufferedWriter bw;
        private long numOfFeatures;

        @Override
        public void setup(Context context) throws IOException {
            pathsFile = new File("paths.txt");
            bw = new BufferedWriter(new FileWriter(pathsFile));
        }

        @Override
        public void reduce(Text key, Iterable<Text> pairsOfNouns, Context context) throws IOException, InterruptedException {
            LinkedList<Text> pairsCopy = new LinkedList<>();
            for (Text nounPair : pairsOfNouns)
                pairsCopy.add(new Text(nounPair));
            if (pairsCopy.size() >= DPmin) {
                numOfFeatures++;
                bw.write(key.toString() + "\n");
                for (Text nounPair : pairsCopy)
                    context.write(nounPair, key);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            System.out.println("Features vector length: " + numOfFeatures);
            AmazonS3 s3 = new AmazonS3Client();
            Region usEast1 = Region.getRegion(Regions.US_EAST_1);
            s3.setRegion(usEast1);
            bw.close();
            HDetector.numOfFeatures = (int)numOfFeatures;
            try {
                System.out.print("Uploading the dependency paths file to S3... ");
                s3.putObject(new PutObjectRequest("dsps162assignment3benasaf/results", "paths.txt", pathsFile));
                System.out.println("Done.");
                pathsFile = new File("numOfFeatures.txt");
                bw = new BufferedWriter(new FileWriter(pathsFile));
                bw.write(numOfFeatures + "\n");
                bw.close();
                System.out.print("Uploading num of features file to S3... ");
                s3.putObject(new PutObjectRequest("dsps162assignment3benasaf/results", "numOfFeatures.txt", pathsFile));
                System.out.println("Done.");
            } catch (AmazonServiceException ase) {
                System.out.println("Caught an AmazonServiceException, which means your request made it "
                        + "to Amazon S3, but was rejected with an error response for some reason.");
                System.out.println("Error Message:    " + ase.getMessage());
                System.out.println("HTTP Status Code: " + ase.getStatusCode());
                System.out.println("AWS Error Code:   " + ase.getErrorCode());
                System.out.println("Error Type:       " + ase.getErrorType());
                System.out.println("Request ID:       " + ase.getRequestId());
            } catch (AmazonClientException ace) {
                System.out.println("Caught an AmazonClientException, which means the client encountered "
                        + "a serious internal problem while trying to communicate with S3, "
                        + "such as not being able to access the network.");
                System.out.println("Error Message: " + ace.getMessage());
            }
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3)
            throw new IOException("Phase 1: supply 3 arguments");
        DPmin = Integer.parseInt(args[2]);
        System.out.println("DPmin is set to: " + DPmin);
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
    }

}
