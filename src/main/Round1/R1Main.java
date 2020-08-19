import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.nio.file.Paths;

public class R1Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setInt("dpMinValue", Integer.parseInt(args[0]));
//        String[] dpPathArr = new String[]{Paths.get(args[3], "DPPaths").toString()};
//        String[] dpPathArr = new String[]{"s3n://dsp-ass3-hadoop2/out1/dpPaths/"};

//        System.out.println(dpPathArr[0]);
//        conf.setStrings("DPOutputPath",dpPathArr);
        Job job = Job.getInstance(conf,"Step1Job");
        job.setJarByClass(R1Main.class);
        job.setMapperClass(R1Mapper.class);
        job.setReducerClass(R1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        MultipleOutputs.addNamedOutput(job,"DP", TextOutputFormat.class,Text.class,Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
