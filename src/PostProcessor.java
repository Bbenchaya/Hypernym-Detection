import java.io.*;

/**
 * Created by asafchelouche on 31/7/16.
 */
public class PostProcessor {

    private static final String PREFIX = "@RELATION nounpair\n\n";
    private static final String POSTFIX = "@ATTRIBUTE ans {true, false}\n\n@DATA\n";

    public static void main(String[] args) throws IOException {
        int vectorLength = Integer.parseInt(args[0]);
        BufferedReader br = new BufferedReader(new FileReader("output/part-r-00000"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("output/processed.arff"));
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
