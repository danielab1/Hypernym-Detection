import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class R2Partitioner extends Partitioner<PairedKey, LongWritable> {
    @Override
    public int getPartition(PairedKey key, LongWritable value, int numPartitions) {
        return Math.abs(key.getDpPath().toString().hashCode()) % numPartitions;
    }
}