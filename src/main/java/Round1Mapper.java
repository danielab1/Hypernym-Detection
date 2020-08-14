import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Round1Mapper extends Mapper<LongWritable, Text, Text, Text> {


    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
    }

    public void map(LongWritable lineId, Text line, Context context) throws IOException, InterruptedException {

        if (line != null) {
            String[] data = line.toString().split("\t");
            String[] syntacticNgram = data[1].split(" ");
            for (String s : syntacticNgram) {
            }
//            }
        }


    }
}
