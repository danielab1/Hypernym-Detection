import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class R4Mapper extends Mapper<Text, Text, FeaturePair, LongWritable> {

    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
    }

    public void map(Text pair, Text coupledDpCount, Context context) throws IOException, InterruptedException {
        String[] coupledDpCounterArr = coupledDpCount.toString().split(",");
        LongWritable dpInd = new LongWritable(Long.parseLong(coupledDpCounterArr[0]));
        LongWritable count = new LongWritable(Long.parseLong(coupledDpCounterArr[1]));
        context.write(new FeaturePair(pair, dpInd), count);

    }
}
