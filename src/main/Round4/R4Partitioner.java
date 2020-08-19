import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class R4Partitioner extends Partitioner<FeaturePair, LongWritable> {

    @Override
    public int getPartition(FeaturePair key, LongWritable longWritable, int numPartitions) {
        return Math.abs(key.getPair().toString().hashCode()) % numPartitions;
    }
}
