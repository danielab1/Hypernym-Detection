import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        AmazonElasticMapReduce emr = AmazonElasticMapReduceClientBuilder
                .standard()
                .withRegion("us-east-1")
                .build();

        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass3-hadoop/dsp-ass3-step1.jar") //parse the biarcs
                .withMainClass("R1Main")
                .withArgs("s3n://dsp-ass3-hadoop/input/", "s3n://dsp-ass3-hadoop/out1/");
        StepConfig step1Config = new StepConfig()
                .withName("Step1Job")
                .withHadoopJarStep(hadoopJarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass3-hadoop/dsp-ass3-step2.jar") // This should be a full map reduce application.
                .withMainClass("R2Main")
                .withArgs("s3n://dsp-ass3-hadoop/input/", "s3n://dsp-ass3-hadoop/out2/");
        StepConfig step2Config = new StepConfig()
                .withName("Step2Job")
                .withHadoopJarStep(hadoopJarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
                .withJar("s3n://dsp-ass3-hadoop/dsp-ass3-step3.jar") // merge Dpath.
                .withMainClass("R3Main")
                .withArgs("s3n://dsp-ass3-hadoop/input/", "s3n://dsp-ass3-hadoop/out3/");
        StepConfig step3Config = new StepConfig()
                .withName("Step3Job")
                .withHadoopJarStep(hadoopJarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");



        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.10.0")
                .withEc2KeyName("dsp-ass2-ec2")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("TestJob")
                .withInstances(instances)
                .withSteps(step1Config)
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withServiceRole("EMR_DefaultRole")
                .withLogUri("s3n://dsp-ass3-hadoop/logs/")
                .withReleaseLabel("emr-5.20.0");



        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
}

