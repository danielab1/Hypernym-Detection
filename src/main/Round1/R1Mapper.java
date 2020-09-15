import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class R1Mapper extends Mapper<LongWritable, Text, Text, Text> {


    public void setup(Context context) throws IOException, InterruptedException {
    }

    public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {
        String[] ngramLine = line.toString().split("\t");
        String[] ngramsEncoded = ngramLine[1].split(" ");
        NgramNode[] ngrams = new NgramNode[ngramsEncoded.length+1];
        for(int i=1; i<=ngramsEncoded.length; i++){
            if(!leglInput(ngramsEncoded[i-1]))
                return;
            ngrams[i] = new NgramNode(ngramsEncoded[i-1]);
        }

        for(int i=1; i< ngrams.length; i++){
            findDepPath(context, ngrams, ngrams[i]);
        }


    }

    private boolean leglInput(String ngramsEncoded){
        String[] ngramArray = ngramsEncoded.split("/");
        if(ngramArray.length!=4)
            return false;
        return isAlphanumeric(ngramArray[0]) && isAlphanumeric(ngramArray[1]);
    }
    private boolean isAlphanumeric(String str) {
        for (int i=0; i<str.length(); i++) {
            char c = str.charAt(i);
            if (c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c <= 0x60) || c > 0x7a)
                return false;
        }

        return true;
    }

    private void findDepPath(Context context, NgramNode[] ngrams, NgramNode startNode) throws IOException, InterruptedException {
        if(!startNode.getPostTag().contains("NN")) return;
        NgramNode curr = ngrams[startNode.getHeadIndex()];
        String dp = startNode.getDepLabel();
        while(curr != null) {
            if(curr.getPostTag().contains("NN")){
                context.write(new Text(dp), new Text(curr.getWord() + "-" + startNode.getWord()));
            }
            dp = dp + "-" + curr.getWord() + "-" + curr.getDepLabel();
            curr = ngrams[curr.getHeadIndex()];
        }

    }
}
