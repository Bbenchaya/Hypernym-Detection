/**
 * Created by asafchelouche on 17/7/16.
 */
public class HDetector {

    private static final String PATHS_LIST_FILENAME = "paths.txt";

    public static void main(String[] args) throws Exception {

        String[] p1args = {"input", "intermediate", args[0], PATHS_LIST_FILENAME};
        String[] p2args = {"intermediate", "output", PATHS_LIST_FILENAME};

        Phase1.main(p1args);
        Phase2.main(p2args);
        String[] ppargs = new String[1];
        ppargs[0] = String.valueOf(Phase1.numOfFeatures);
        PostProcessor.main(ppargs);

    }

}
