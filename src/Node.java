import java.util.LinkedList;
import java.util.List;

/**
 * Created by asafchelouche on 26/7/16.
 * A node in the tree representation of a syntactic Ngram.
 */

class Node {

    private String w1;
    private String w2;
    private int index;
    private String pos_tag; // part of speech
    private String dep_label; // stanford dependency
    private List<Node> children;


    public Node(String w1, String w2, int index, String pos_tag, String dep_label) {
        this.w1 = w1;
        this.w2 = w2;
        this.index = index;
        this.pos_tag = pos_tag;
        this.dep_label = dep_label;
        children = new LinkedList<Node>();
    }

    public void addChild(Node child) {
        children.add(child);
    }

    public int getIndex() {
        return index;
    }

    public String getW2() {
        return w2;
    }

    public String getW1() {
        return w1;
    }

    public String getDependencyPath() {
        // TODO implement
        return "";
    }

}
