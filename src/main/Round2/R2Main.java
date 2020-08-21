import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class R2Main { //ROUND for merge all the DP to one file.
     public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "Step2Job");
         job.setJarByClass(R2Main.class);
         job.setMapperClass(R2Mapper.class);
         job.setMapOutputKeyClass(Text.class);
         job.setMapOutputValueClass(Text.class);

         job.setReducerClass(R2Reducer.class);
         job.setNumReduceTasks(1);

         job.setOutputKeyClass(LongWritable.class);
         job.setOutputValueClass(Text.class);

         FileInputFormat.addInputPath(job, new Path(args[1]));
         FileOutputFormat.setOutputPath(job, new Path(args[2]));

         job.setInputFormatClass(KeyValueTextInputFormat.class);
         MultipleOutputs.addNamedOutput(job,"featureVectorSize", TextOutputFormat.class,Text.class,Text.class);
         job.setOutputFormatClass(TextOutputFormat.class);

         System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}
