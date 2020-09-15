import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;


public class R2Reducer extends Reducer<Text,Text,LongWritable, Text> {
    private long index = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(index++),key);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    }


}
