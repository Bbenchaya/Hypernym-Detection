import com.amazonaws.transform.MapEntry;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.J48;
import weka.core.Debug;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;
import weka.core.converters.ConverterUtils.DataSource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Created by asafchelouche on 8/8/16.
 */
public class ClassifierTester {

    public static void main(String[] args) throws Exception {
        // train
        Instances train = DataSource.read("processed_single_corpus.arff");
        train.setClassIndex(train.numAttributes() - 1);
        Evaluation crossValidate = new Evaluation(train);
        Classifier tree = new J48();
        crossValidate.crossValidateModel(tree, train, 10, new Debug.Random(1));
        System.out.println(crossValidate.toSummaryString("\nResults\n\n", false));
        System.out.println(crossValidate.toClassDetailsString("\nStatistics\n\n"));

        // test
        tree.buildClassifier(train);
        Instances testInput = DataSource.read("processed_single_corpus.arff");
        testInput.setClassIndex(train.numAttributes() - 1);
        Instances testOutput = new Instances(testInput);
        for (int i = 0; i < testInput.numInstances(); i++) {
            double clsLabel = tree.classifyInstance(testInput.instance(i));
            testOutput.instance(i).setClassValue(clsLabel);
        }
        ConverterUtils.DataSink.write("testOutput.arff", testOutput);

        testOutput = DataSource.read("testOutput.arff");

        if (testOutput.size() != train.size())
            throw new Exception("Training set and tagged set differ in number of entries.");

        HashMap<String, String> tp = new HashMap<>(10);
        HashMap<String, String> fp = new HashMap<>(10);
        HashMap<String, String> tn = new HashMap<>(10);
        HashMap<String, String> fn = new HashMap<>(10);
        BufferedReader br = new BufferedReader(new FileReader("output/part-r-00000"));
        String line;
        while (!(line = br.readLine()).contains(",")) {
            System.out.println(line);
        }

        for (int i = 0; i < train.size(); i++, line = br.readLine()) {
            String trainSetEntry = train.get(i).toString();
            String testSetEntry = testOutput.get(i).toString();
            boolean trainTruthValue = trainSetEntry.substring(trainSetEntry.lastIndexOf(",") + 1).equals("true");
            boolean testTruthValue = testSetEntry.substring(testSetEntry.lastIndexOf(",") + 1).equals("true");
            String nounPair = line.substring(0, line.indexOf("\t"));
            String vector = line.substring(line.indexOf("\t") + 1);
            if (trainTruthValue && testTruthValue && tp.size() < 10)
                tp.put(nounPair, vector);
            else if (!trainTruthValue && testTruthValue && fp.size() < 10)
                fp.put(nounPair, vector);
            else if (!trainTruthValue && !testTruthValue && tn.size() < 10)
                tn.put(nounPair, vector);
            else if (trainTruthValue && !testTruthValue && fn.size() < 10)
                fn.put(nounPair, vector);
        }

        System.out.println("\n\nTrue positives:");
        for (Map.Entry<String, String> entry : tp.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());
        System.out.println("\n\nFalse positives:");
        for (Map.Entry<String, String> entry : fp.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());
        System.out.println("\n\nTrue negatives:");
        for (Map.Entry<String, String> entry : tn.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());
        System.out.println("\n\nFalse negatives:");
        for (Map.Entry<String, String> entry : fn.entrySet())
            System.out.println(entry.getKey() + "\n" + entry.getValue());


    }

}
