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

import java.io.*;
import java.util.LinkedList;

/**
 * Created by asafchelouche on 26/7/16.
 */

public class Phase1 {

    private static int DPmin;
    private static String pathsFilename;
    private static FileSystem hdfs;
    static long numOfFeatures;

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

        private Path hdfsFile;
        private OutputStream out;
        private BufferedWriter bw;

        @Override
        public void setup(Context context) throws IOException {
            hdfsFile = new Path(pathsFilename);
            out = hdfs.create(hdfsFile);
            bw = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));

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
            bw.close();
            out.close();
            hdfs.close();
        }

    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4)
            throw new IOException("Phase 1: supply 4 arguments");
        DPmin = Integer.parseInt(args[2]);
        pathsFilename = args[3];
        numOfFeatures = 0;
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
        hdfs = FileSystem.get(conf);
        if (job.waitForCompletion(true))
            System.out.println("Phase 1: job completed successfully");
        else
            System.out.println("Phase 1: job completed unsuccessfully");
        Counter counter = job.getCounters().findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_INPUT_RECORDS");
        System.out.println("Num of pairs sent to reducers in phase 1: " + counter.getValue());
    }

}
