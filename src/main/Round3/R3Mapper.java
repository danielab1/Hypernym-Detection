import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class R3Mapper extends Mapper<Text, Text, PairedKey, LongWritable> {


//    Reducer ((dbPath, pair), [count])
//            //if this is the first time this dbPath appears, save it's row line in s3
//            content.write(
    public void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
    }

    public void map(Text dpPath, Text pair, Context context) throws IOException, InterruptedException {
        context.write(new PairedKey(dpPath, pair), new LongWritable(1));

    }
}

/*
cease for some time

Round1
    Mapper
        key: (some-time, noun-np-noun, 1)
        key: (some-time, noun-vb-noun, 1)
        key: (some-time, noun-vb-noun, 1)
    Reduce
        key: (some-time, noun-np-noun) val: [1,1,1]
        key: (some-time, noun-vb-noun) val: [1,12,1]
        content.write((noun-vb-noun, some-time), 22)

Round2
    Mapper
        content.write(noun-vb-noun, (some-time, 22))
    Reducer (dpPath, [(pair, count),...])
            if(dpmin<=len)
                contentDP.write(dpPath)
                for each value
                    content.write(dbPath, pair, count)

Round3
    Mapper
        content.write(dpPath, (pair, 22))
    Reducer (dpPath, [(pair, count),...])
            if(dpmin<=len)
                for each value
                    content.write(dbPath, pair, count)
                           [noun-np-noun, noun-vb-noun...]
some-time, feature-vector: [12,0,0,32,1,43,...], ?annotation?


Round1
    Set<Text> dpSet
    Mapper
        content.write(
        key: noun-np-noun, value: some-time
        key: noun-vb-noun, value: some-time
        key: noun-vb-noun, value: some-time)

    Reducer (dpPath, pair[])
        valid = false
        for each pair in values
            if(!dpSet.has(pair) && dpSet.length < dpMin)
                dpSet.add(pair)
            else if (dpMin <= dpSet.length)
                contentDP.write(dpPath, (*,*))
                valid=true;
                break
        if valid => for each pair in values
            content.write(dpPath, pair)

Round2
    Setup
        S3dpPaths

    Mapper (dpPath, pair)
        content.write((dpPath, pair, ind), 1)
        // can't do (dbPath, pair) because
        this way I will have to go through all the list to count how many times
        each pair appeared as this dp


    Reducer ((dbPath, pair, ind), [count])
        //if this is the first time this dbPath appears, save it's row line in s3
        content.write(

    some-time, feature-vector: [12,0,0,32,1,43,...], ?annotation?

    Final Output:

@some-time 0,2,3,0,1,3...
some-time 0, 0
some-time 2, 1
some-time 3, 2
some-time 0, 3

 */
