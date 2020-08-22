import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;


public class R1Reducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs mos;
    private int dpMin;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);
        dpMin = context.getConfiguration().getInt("dpMinValue",5);
    }

    @Override
    public void reduce(Text dpPath, Iterable<Text> nounPairs, Context context) throws IOException,  InterruptedException {
        Set<String> set = new HashSet<>();
        boolean valid = false;
        for(Text pair: nounPairs) {
            set.add(pair.toString());
            if (dpMin <= set.size()) {
                valid = true;
                mos.write("DP", dpPath, new Text(""), "/dp/part");
                break;
            }
        }

        if(valid){
            for (Text nounPair : nounPairs) {
                context.write(dpPath, nounPair);
            }
            for(String pair : set){
                context.write(dpPath, new Text(pair));
            }


        }


    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {
        mos.close();
    }
}
