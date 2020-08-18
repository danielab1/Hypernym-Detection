import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class R2Reducer extends Reducer<Text,Text,LongWritable, Text> {
    private long index = 0;

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(new LongWritable(index),key);
        index++;
    }
}
