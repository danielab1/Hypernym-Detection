import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class R3Mapper extends Mapper<Text, Text, PairedKey, LongWritable> {

    public void setup(Context context) throws IOException, InterruptedException {
    }

    public void map(Text dpPath, Text pair, Context context) throws IOException, InterruptedException {
        context.write(new PairedKey(dpPath, pair), new LongWritable(1));

    }
}
