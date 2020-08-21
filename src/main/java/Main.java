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
import com.google.inject.internal.cglib.proxy.$ProxyRefDispatcher;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
//        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
//                .standard()
//                .withRegion("us-east-1")
//                .build();
//
//        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
//                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step1.jar") //parse the biarcs
//                .withMainClass("R1Main")
//                .withArgs("5", "s3n://dsp-ass3-hadoop2/input/", "s3n://dsp-ass3-hadoop2/out1/");
//        StepConfig step1Config = new StepConfig()
//                .withName("Step1Job")
//                .withHadoopJarStep(hadoopJarStep1)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");
//
//        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
//                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step2.jar") // merge Dpath.
//                .withMainClass("R2Main")
//                .withArgs("s3n://dsp-ass3-hadoop2/dp/", "s3n://dsp-ass3-hadoop2/dp_merge/");
//
//
//        StepConfig step2Config = new StepConfig()
//                .withName("Step2Job")
//                .withHadoopJarStep(hadoopJarStep2)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");
//
//        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
//                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step3.jar") // create feature vector
//                .withMainClass("R3Main")
//                .withArgs("s3n://dsp-ass3-hadoop2/out1/", "s3n://dsp-ass3-hadoop2/out3/");
//        StepConfig step3Config = new StepConfig()
//                .withName("Step3Job")
//                .withHadoopJarStep(hadoopJarStep3)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");
//
//        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
//                .withJar("s3n://dsp-ass3-hadoop2/jars/dsp-ass3-step4.jar") // create feature vector
//                .withMainClass("R4Main")
//                .withArgs("s3n://dsp-ass3-hadoop2/out3/", "s3n://dsp-ass3-hadoop2/out4/");
//        StepConfig step4Config = new StepConfig()
//                .withName("Step4Job")
//                .withHadoopJarStep(hadoopJarStep4)
//                .withActionOnFailure("TERMINATE_JOB_FLOW");
//
//        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
//                .withInstanceCount(2)
//                .withMasterInstanceType(InstanceType.M4Large.toString())
//                .withSlaveInstanceType(InstanceType.M4Large.toString())
//                .withHadoopVersion("2.10.0")
//                .withEc2KeyName("dsp-ass3-ec2")
//                .withKeepJobFlowAliveWhenNoSteps(false)
//                .withPlacement(new PlacementType("us-east-1a"));
//
//        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
//                .withName("TestJob")
//                .withInstances(instances)
//                .withSteps(step3Config,step4Config)
//                .withJobFlowRole("EMR_EC2_DefaultRole")
//                .withServiceRole("EMR_DefaultRole")
//                .withLogUri("s3n://dsp-ass3-hadoop2/logs/")
//                .withReleaseLabel("emr-5.20.0");
//
//
//        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
//        String jobFlowId = runJobFlowResult.getJobFlowId();
//        System.out.println("Ran job flow with id: " + jobFlowId);


        Set<Text> set = new HashSet<>(5);
        boolean valid = false;

        File myObj = new File(args[0]);
        Scanner myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            String[] ngramLine = data.split("\t");
            String[] ngramsEncoded = ngramLine[1].split(" ");
            NgramNode[] ngrams = new NgramNode[ngramsEncoded.length + 1];
            for (int i = 1; i <= ngramsEncoded.length; i++) {
                ngrams[i] = new NgramNode(ngramsEncoded[i - 1]);
            }

            for (int i = 1; i < ngrams.length; i++) {
                findDepPath(ngrams, ngrams[i]);
            }
        }
        myReader.close();

        myObj = new File("output.txt");
        myReader = new Scanner(myObj);
        while (myReader.hasNextLine()) {
            String data = myReader.nextLine();
            String[] str = data.split("\t");

                if (!set.contains(new Text(str[1])) && set.size() < 5) {
                    set.add(new Text(str[1]));
                } else if (5 <= set.size()) {
                    valid = true;
                    System.out.println((str[0]));
                }

            if (valid) {
                System.out.println(str[0] + " " + str[1]) ;
            }
        }
        myReader.close();
    }
        private static void findDepPath (NgramNode[]ngrams, NgramNode startNode) throws IOException {
            if (!startNode.getPostTag().contains("NN")) return;
            NgramNode curr = ngrams[startNode.getHeadIndex()];
            String dp = startNode.getPostTag();
            BufferedWriter writer = new BufferedWriter(new FileWriter("output.txt", true));
            while (curr != null) {
                dp = curr.getPostTag() + "-" + dp;
                if (curr.getPostTag().contains("NN")) {
                    writer.append(dp).append("\t").append(curr.getWord()).append("-").append(startNode.getWord()).append("\n");
                }
                curr = ngrams[curr.getHeadIndex()];
            }
            writer.close();

        }
    }




