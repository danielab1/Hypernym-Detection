import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class R1Reducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs mos;
    private int dpMin;
//    private String DPOutputPath;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
        dpMin = context.getConfiguration().getInt("dpMinValue",5);
//        String[] outputs = context.getConfiguration().getStrings("DPOutputPath");;
//        DPOutputPath = outputs[0];
    }

    @Override
    public void reduce(Text dpPath, Iterable<Text> nounPairs, Context context) throws IOException,  InterruptedException {
        Set<Text> set = new HashSet<>(dpMin);
        boolean valid = false;
        for(Text pair: nounPairs){
            if(!set.contains(pair) && set.size() < dpMin){
                set.add(pair);
            } else if(dpMin <= set.size()){
                valid = true;
                mos.write("DP", dpPath, new Text(""), "/dp/part");
                break;
            }
        }
        if(valid){
            for(Text pair: nounPairs){
                context.write(dpPath, pair);
            }
        }
    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {
        mos.close();
    }
}
