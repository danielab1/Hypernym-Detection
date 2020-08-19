import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairedKey implements WritableComparable {
    private Text dpPath;
    private Text pair;

    public PairedKey(Text dpPath, Text pair) {
        this.dpPath = dpPath;
        this.pair = pair;
    }

    public PairedKey(String dpPath, String pair){
        this.dpPath = new Text(dpPath);
        this.pair = new Text(pair);
    }

    public PairedKey() {
        this.dpPath = new Text("");
        this.pair = new Text("");
    }

    public Text getDpPath() {
        return dpPath;
    }

    public void setDpPath(Text dpPath) {
        this.dpPath = dpPath;
    }

    public Text getPair() {
        return pair;
    }

    public void setPair(Text pair) {
        this.pair = pair;
    }



    @Override
    public int compareTo(Object o) {
        PairedKey otherPair = (PairedKey) o;
        if(dpPath.toString().equals(otherPair.getDpPath().toString())){
            return pair.toString().compareTo(otherPair.getPair().toString());
        }
        return this.dpPath.compareTo(otherPair.getDpPath());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dpPath.write(dataOutput);
        pair.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        dpPath.readFields(dataInput);
        pair.readFields(dataInput);
    }
}
