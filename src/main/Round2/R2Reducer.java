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


public class R2Reducer extends Reducer<PairedKey, LongWritable, Text, Text> {
    private AmazonS3 s3;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
    }

    @Override
    public void reduce(PairedKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
//        System.out.format("Downloading %s from S3 bucket %s...\n", filename, bucketName);
//        S3Object o = null;
//
//        try {
//            S3Object object = s3.getObject(new GetObjectRequest("arn:aws:s3:::dsp-ass3-hadoop", "out"));
//
//            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
//
//            while (true) {
//                String line = reader.readLine();
//                if (line == null)
//                    break;
//            }
//
//        } catch (AmazonServiceException e) {
//            System.err.println(e.getErrorMessage());
//            System.exit(1);
//        } catch (IOException e) {
//            System.err.println(e.getMessage());
//            System.exit(1);
//        }
    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {

    }


}
