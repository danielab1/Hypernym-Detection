import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;


public class R2Reducer extends Reducer<Text,Text,LongWritable, Text> {
    private long index = 0;
    private MultipleOutputs mos;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<>(context);

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(index++),key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.write("featureVectorSize",index,new Text(""),"/featureVectorSize/part");
        mos.close();

    }


}
