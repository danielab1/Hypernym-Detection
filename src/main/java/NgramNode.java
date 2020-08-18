public class NgramNode {

    private String word;
    private String postTag;
    private String depLabel;
    private int headIndex;

    public NgramNode(String ngram){
        Stemmer stemmer = new Stemmer();
        String[] fields = ngram.split("/");
        char[] wordAsChar = fields[0].toCharArray();
        if(fields[0].length() == 0){
            postTag = "";
            return;
        }
        stemmer.add(wordAsChar, wordAsChar.length);
        stemmer.stem();
        word = stemmer.toString();
        postTag = fields[1];
        depLabel = fields[2];
        headIndex = Integer.parseInt(fields[3]);
    }

    public NgramNode(String word, String postTag, String depLabel, int headIndex) {
        this.word = word;
        this.postTag = postTag;
        this.depLabel = depLabel;
        this.headIndex = headIndex;
    }

    public NgramNode(){
        this.word = null;
        this.postTag = null;
        this.depLabel = null;
        this.headIndex = -1;
    }

    public String getWord() {
        return word;
    }

    public String getPostTag() {
        return postTag;
    }

    public String getDepLabel() {
        return depLabel;
    }

    public int getHeadIndex() {
        return headIndex;
    }

    public String toString(){
        return word + " " + postTag + " " + depLabel + " " + String.valueOf(headIndex);
    }
}
