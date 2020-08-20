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
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class R4Reducer extends Reducer<FeaturePair, LongWritable, Text, Text> {
    private AmazonS3 s3;
    private Map<String, Boolean> annotatedSet;
    private String lastPair;
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        s3 = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .build();
        lastPair = null;
        annotatedSet = new HashMap<>();
        loadAnnotatedSet();
    }

    @Override
    public void reduce(FeaturePair key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
        String keyPair = key.getPair().toString();
        if(lastPair == null || !keyPair.equals(lastPair)){
            String annotation = "NA";
            if(annotatedSet.containsKey(keyPair)){
                annotation = annotatedSet.get(keyPair).toString();
            }
            context.write(key.getPair(), new Text("-1,"+annotation));
            lastPair = keyPair;
        }

        for(LongWritable count: values){
            context.write(key.getPair(), new Text(key.getDpInd()+","+count));
        }
    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {

    }

    private String stemWord(String wordRaw){
        Stemmer stemmer = new Stemmer();
        char[] wordAsChar = wordRaw.toCharArray();
        stemmer.add(wordAsChar, wordAsChar.length);
        stemmer.stem();
        return stemmer.toString();
    }
    private void loadAnnotatedSet(){
        try {
            S3Object object = s3.getObject(new GetObjectRequest("dsp-ass3-hadoop2", "/annotated/hypernym.txt"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));

            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;

                String[] content = line.split("\t");
                String pair = stemWord(content[0])+" "+stemWord(content[1]);
                annotatedSet.putIfAbsent(pair, Boolean.parseBoolean(content[2]));

            }

        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
