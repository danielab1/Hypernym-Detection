import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FeaturePair implements WritableComparable {

    private Text pair;
    private LongWritable dpInd;

    public FeaturePair(Text pair, LongWritable dpInd) {
        this.pair = pair;
        this.dpInd = dpInd;
    }

    public FeaturePair() {
       this.pair = new Text("");
       this.dpInd = new LongWritable(-1);
    }

    public Text getPair() {
        return pair;
    }

    public LongWritable getDpInd() {
        return dpInd;
    }

    @Override
    public int compareTo(Object o) {
        FeaturePair otherPair = (FeaturePair) o;
        Text oPair = otherPair.getPair();
        LongWritable oDpInd = otherPair.getDpInd();
        if(oPair.toString().equals(pair.toString())){
            return (int)(dpInd.get()-oDpInd.get());
        } else {
            return pair.toString().compareTo(oPair.toString());
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pair.write(dataOutput);
        dpInd.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pair.readFields(dataInput);
        dpInd.readFields(dataInput);
    }
}
