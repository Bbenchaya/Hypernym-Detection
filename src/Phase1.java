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
import java.util.HashSet;

/**
 * Created by asafchelouche on 26/7/16.
 */

public class Phase1 {

    private static int DPmin;

    public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

        private Stemmer stemmer;
        private final String REGEX = "[^a-zA-Z ]+";

        /**
         * Setup the Mapper node.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            stemmer = new Stemmer();
        }

        /**
         * Map a key-value pair.
         * @param key the line index of the current line of the input file.
         * @param value the line contents.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
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

        /**
         * Transforms a biarc into a an array of Nodes
         * @param parts an array of Strings, each index being a biarc
         * @return an array of Nodes, each one containing a biarc in an accessible container.
         */
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

        /**
         * Transforms an array of Nodes into a tree, which represents the dependencies defined in the original biarc.
         * @param nodes an array of Nodes.
         * @return the root of the tree.
         */
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

        /**
         * A recursive method to find all shortest paths between nouns in a syntactic tree.
         * @param node a node that is inquired as to being a start or end of a shortest path.
         * @param acc an accumulator that holds the shortest path so far as a String.
         * @param firstWord the first word of the noun pair.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
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

        /**
         * Setup up a Reducer node.
         * @param context the Map-Reduce job context.
         * @throws IOException
         */
        @Override
        public void setup(Context context) throws IOException {
            pathsFile = new File("paths.txt");
            bw = new BufferedWriter(new FileWriter(pathsFile));
            numOfFeatures = 0;
        }

        /**
         * Counts how many noun pairs exist for a particular dependency path. If there are more than DPmin, this
         * dependency path would be treated as a feature, thus taking an index in the features vector for each noun pair
         * in the corpus.
         * Each dependency path that is counted as a feature would be written to a local file.
         * @param key a dependency path.
         * @param nounPairs all pairs of nouns which appear in this dependency path in the corpus.
         * @param context the Map-Reduce job context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> nounPairs, Context context) throws IOException, InterruptedException {
            HashSet<Text> set = new HashSet<>(DPmin);
            for (Text nounPair : nounPairs) {
                if (set.size() == DPmin)
                    break;
                else if (set.contains(nounPair))
                    continue;
                else
                    set.add(nounPair);
            }
            if (set.size() >= DPmin) {
                bw.write(key.toString() + "\n");
                numOfFeatures++;
                for (Text nounPair : nounPairs)
                    context.write(nounPair, key);
            }
        }

        /**
         * Writes 2 files to an S3 bucket:
         * 1. the local file, containing all dependency paths that are counted as features
         * 2. a file which contains the number of features, i.e. the number of dependency paths with more than DPmin noun pairs
         * @throws IOException
         */
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

    /**
     * Main method for this Map-Reduce step. Extracts all dependency paths between pairs of nouns that are to be treated
     * as features in a features vector, in preperation for post-processing in WEKA.
     * @param args an array of 3 Strings: input path, output path, DPmin.
     * @throws Exception
     */
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
        System.out.println("Number of key-value pairs sent to reducers in phase 1: " + counter.getValue());
    }

}
