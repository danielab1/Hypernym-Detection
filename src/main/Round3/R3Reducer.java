import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.security.auth.callback.Callback;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;


public class R3Reducer extends Reducer<PairedKey, LongWritable, Text, Text> {
    private AmazonS3 s3;
    private BufferedReader buff;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
        S3Object s3Object = s3.getObject(new GetObjectRequest("dsp-ass3-hadoop", "out2/"));
    }

    @Override
            //(dpPath, [(pair, count),...])
    public void reduce(PairedKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
        boolean found = false;
        long count = 0;
        for(LongWritable value : values ) {
            count = count + value.get();
        }
    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {

    }


}
