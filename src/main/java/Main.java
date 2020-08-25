import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.common.collect.HashBasedTable;
import com.google.inject.internal.cglib.proxy.$ProxyRefDispatcher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    private static Hashtable<String,Boolean> annotatedSet = new Hashtable<>();
    public static void main(String[] args) throws IOException, InterruptedException {
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion("us-east-1")
                .build();

        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step1.jar") //parse the biarcs
                .withMainClass("R1Main")
                .withArgs("5", "s3n://dsp-ass3-hadoop2/input/", "s3n://dsp-ass3-hadoop2/out1/");
        StepConfig step1Config = new StepConfig()
                .withName("Step1Job")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step2.jar") // merge Dpath.
                .withMainClass("R2Main")
                .withArgs("s3n://dsp-ass3-hadoop2/dp/", "s3n://dsp-ass3-hadoop2/dp_merge/");


        StepConfig step2Config = new StepConfig()
                .withName("Step2Job")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step3.jar") // create feature vector
                .withMainClass("R3Main")
                .withArgs("s3n://dsp-ass3-hadoop2/out1/", "s3n://dsp-ass3-hadoop2/out3/");
        StepConfig step3Config = new StepConfig()
                .withName("Step3Job")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step4.jar") // create feature vector
                .withMainClass("R4Main")
                .withArgs("s3n://dsp-ass3-hadoop2/out3/", "s3n://dsp-ass3-hadoop2/out4/");
        StepConfig step4Config = new StepConfig()
                .withName("Step4Job")
                .withHadoopJarStep(hadoopJarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(8)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.10.0")
                .withEc2KeyName("dsp-ass3-ec2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("dspAss3")
                .withInstances(instances)
                .withSteps(step1Config,step2Config,step3Config,step4Config)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withLogUri("s3n://dsp-ass3-hadoop2/logs/")
                .withReleaseLabel("emr-5.20.0");


        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);


//        String stemmed = stemWord("schizophren");
//        String stemmed2 = stemWord("schizophrenia");
//        System.out.println(stemmed + " " + stemmed2);
//        System.out.println(Boolean.parseBoolean("True"));
//        File myObj = new File("hypernym.txt");
//        Scanner myReader = new Scanner(myObj);
//
//        while (myReader.hasNextLine()) {
//            String line = myReader.nextLine();
//            String[] content = line.split("\t");
//            String pair = stemWord(content[0])+" "+stemWord(content[1]);
//            annotatedSet.putIfAbsent(pair, Boolean.parseBoolean(content[2]));
//
//        }
//        myReader.close();
//        myObj = new File("part-r-00012");
//        myReader = new Scanner(myObj);
//         String lastPair = null;
//        while (myReader.hasNextLine()) {
//            String line = myReader.nextLine();
//            String[] content = line.split("\t");
//            String keyPair = content[0];
//            if (!keyPair.equals(lastPair)) {
//                String annotation = "NA";
//                if (containsKey(keyPair)) {
//                    annotation = getKey(keyPair);
//                }
//                System.out.println(keyPair + " " + "-1," + annotation);
//                lastPair = keyPair;
//            }
//
//                System.out.println(keyPair + " " + content[1]);
//
//        }
//        myReader.close();
//
//        myObj = new File("output.txt");
//        myReader = new Scanner(myObj);
//        while (myReader.hasNextLine()) {
//            String data = myReader.nextLine();
//            String[] str = data.split("\t");
//            String currDp = str[0];
//            boolean exist = set.containsKey(currDp);
//                if (!exist) {
//                    HashSet<Text> l = new HashSet<>();
//                    l.add(new Text(str[1]));
//                    set.put(str[0],l);
//                } else {
//                    set.get(currDp).add(new Text(str[1]));
//                }
//            }
//
//        myReader.close();
//        for( String key : set.keySet()) {
//            if(set.get(key).size() >=5) {
//                System.out.println("------------------------" + key);
//                for (Text pair : set.get(key)) {
//                    System.out.println(pair.toString());
//                }
//            }
//        }
//        HashSet<Text> hashSet =  new HashSet<>();
//        hashSet.add(new Text("abak-cast"));
//        hashSet.add(new Text("aahaara-intak"));
//        hashSet.add(new Text("abhasa-appear"));
//        hashSet.add(new Text("worship-imag"));
//        for (Iterator<Text> it = hashSet.iterator(); it.hasNext(); ) {
//            Text nounPair = it.next();
//            System.out.println(nounPair);
//        }
//        System.out.println(hashSet.toString());
//        hashSet.add(new Text("abak-cast"));
//        System.out.println(hashSet.toString());


    }
    private static String stemWord(String wordRaw){
        Stemmer stemmer = new Stemmer();
        char[] wordAsChar = wordRaw.toCharArray();
        stemmer.add(wordAsChar, wordAsChar.length);
        stemmer.stem();
        return stemmer.toString();
    }
    private static boolean containsKey(String key){
        String[] pair = key.split("-");
        String keyString = pair[0] + " " + pair[1];
        return annotatedSet.containsKey(keyString);
    }
    public static String getKey(String key){
        String[] pair = key.split("-");
        String keyString = pair[0] + " " + pair[1];
        String reverseKey = pair[1] + " " + pair[0];
        Boolean res = annotatedSet.get(keyString);
        Boolean reverseRes = annotatedSet.get(reverseKey);
        return res== null ? reverseRes.toString() : res.toString();
    }
    }




