import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


public class R1Reducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs mos;
    private int dpMin;
    private Set<Text> set;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        set = new HashSet<>();
        mos = new MultipleOutputs(context);
        dpMin = context.getConfiguration().getInt("dpMinValue",5);
    }

    @Override
    public void reduce(Text dpPath, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
        boolean valid = false;
        for(Text pair: values){
            if(!set.contains(pair) && set.size() < dpMin){
                set.add(pair);
            } else if(dpMin <= set.size()){
                valid = true;
                mos.write("DP", dpPath, new Text(""));
                break;
            }
        }
        if(valid){
            for(Text pair: values){
                context.write(dpPath, pair);
            }
        }
    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {

    }
}