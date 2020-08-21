import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;


public class R3Reducer extends Reducer<PairedKey, LongWritable, Text, Text> {
    private AmazonS3 s3;
    private Text lastDp;
    private long dpInd;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
        lastDp = new Text("");
    }

    @Override
    public void reduce(PairedKey key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {

        long count = 0;
        for(LongWritable value : values ) {
            count = count + value.get();
        }
        if(!lastDp.toString().equals(key.getDpPath().toString())){
            updateDpPathInd(key);
        }
        context.write(key.getPair(), new Text(dpInd+","+count));
    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {

    }

    private void updateDpPathInd(PairedKey key){
        ObjectListing listing = s3.listObjects( "dsp-ass3-hadoop2", "dp_merge/" );
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        for(S3ObjectSummary summary: summaries){
            searchDpInBucket(key, summary.getKey());
        }
    }

    private void searchDpInBucket(PairedKey key, String pathToFile){

        try {
            S3Object object = s3.getObject(new GetObjectRequest("dsp-ass3-hadoop2", pathToFile));
            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
            boolean found = false;
            while (!found) {
                String line = reader.readLine();
                if (line == null)
                    break;

                String[] content = line.split("\t");
                long ind = Long.parseLong(content[0]);
                String dpPath = content[1];
                if((found = dpPath.equals(key.getDpPath().toString()))){
                    dpInd = ind;
                    lastDp = new Text(key.getDpPath().toString());
                }

            }

        } catch (AmazonServiceException e) {
            System.out.println(e.getErrorMessage());
            System.out.println(e.getMessage());
            System.exit(2);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
