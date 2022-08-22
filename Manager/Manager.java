import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InstanceStateChange;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

//
// Decompiled by Procyon v0.5.36
//

public class Manager
{
    static SqsClient sqs;
    static S3Client s3;
    static Ec2Client ec2;
    public static String URLfromApp;
    public static final ReentrantLock workersLock;
    public static AtomicInteger workersNumber;
    public static int maxWorkers;
    public static List<String> workers;
    public static String toWorkers;
    public static String fromWorkers;
    public static String URLtoApp;
    private static ThreadPoolExecutor poolExecutor;
    private static AtomicBoolean terminateF;
    public static int n;
    private static String ami;
    public static AtomicBoolean done;
    public static String jarsBucket = "https://myappjarbucket.s3.amazonaws.com/worker.jar";
    static {
        Manager.sqs = (SqsClient.builder().region(Region.US_EAST_1)).build();
        Manager.s3 = (S3Client.builder().region(Region.US_EAST_1)).build();
        Manager.ec2 = (Ec2Client.builder().region(Region.US_EAST_1)).build();
        workersLock = new ReentrantLock();
        Manager.workersNumber = new AtomicInteger(0);
        Manager.maxWorkers = 15;
        Manager.workers = new LinkedList<String>();
        poolExecutor = new ThreadPoolExecutor(1, 1, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        Manager.terminateF = new AtomicBoolean(false);
        Manager.n = 0;
        Manager.ami = "";
        Manager.done = new AtomicBoolean(false);
    }

    public static void main(final String[] args) {
        System.out.println("Manager start");
        Manager.ami = args[0];
        Manager.n = Integer.parseInt(args[1]);
        try {
            final CreateQueueRequest requestTo = (CreateQueueRequest)CreateQueueRequest.builder().queueName("To_workers").build();
            Manager.sqs.createQueue(requestTo);
            final CreateQueueRequest requestFrom = (CreateQueueRequest)CreateQueueRequest.builder().queueName("From_workers").build();
            Manager.sqs.createQueue(requestFrom);
            final CreateQueueRequest requestApp = (CreateQueueRequest)CreateQueueRequest.builder().queueName("App_To_Manager").build();
            Manager.sqs.createQueue(requestApp);
             poolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        }
        catch (QueueNameExistsException e) {
            throw e;
        }
        Manager.toWorkers = Manager.sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("To_workers").build()).queueUrl();
        Manager.fromWorkers = Manager.sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("From_workers").build()).queueUrl();
        Manager.URLfromApp = Manager.sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("App_To_Manager").build()).queueUrl();
        Manager.URLtoApp = Manager.sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("Manager_To_App").build()).queueUrl();
        while(!Manager.terminateF.get()){
            final ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(Manager.URLfromApp)
            .visibilityTimeout(Integer.valueOf(60)).build();
            List<Message> messages = Manager.sqs.receiveMessage(receiveRequest).messages();
            for (Message m : messages) {
                String[] split1 = m.body().split("\t");
                if (split1[0].equals("terminate")) {
                    System.out.println("Received terminate flag!");
                    Manager.terminateF.set(true);
                    poolExecutor.shutdown(); //finishing the old tasks and no more new tasks will be accepted
                    Manager.terminate(m);
                    System.exit(0);
                }
                //managerRunnable manger =  new managerRunnable(m);
                //manger.run();
                poolExecutor.execute(new managerRunnable(m));
            }
        }
    }


    public static void terminate(final Message m) {
        Manager.terminateF.set(true);
        System.out.println("Termination sequence is starting by the manager");

        while (!poolExecutor.isTerminated()) {
            try {
                Thread.sleep(2000L);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        Manager.done.set(true);
//        Manager.workersLock.lock();
        for (final String worker : Manager.workers) {
            terminateEC2(Manager.ec2, worker);
        }
//        Manager.workersLock.unlock();
        String[] split = m.body().split("\t");
        final SendMessageRequest send = (SendMessageRequest)SendMessageRequest.builder().queueUrl(Manager.URLtoApp).messageBody("terminated\t").build();
        Manager.sqs.sendMessage(send);
        deleteSQS(Manager.sqs, "To_workers");
        deleteSQS(Manager.sqs, "From_workers");
        Manager.sqs.close();
        Manager.ec2.close();
        Manager.s3.close();
    }
    public static void terminateEC2(final Ec2Client ec2, final String instanceID) {
    try {
        final TerminateInstancesRequest ti = (TerminateInstancesRequest)TerminateInstancesRequest.builder().instanceIds(new String[] { instanceID }).build();
        final TerminateInstancesResponse response = ec2.terminateInstances(ti);
        final List<InstanceStateChange> list = (List<InstanceStateChange>)response.terminatingInstances();
        for (final InstanceStateChange sc : list) {
            System.out.println("The ID of the terminated instance is " + sc.instanceId());
        }
    }
    catch (Ec2Exception e) {
        System.err.println("EC2 ERROR:");
        System.err.println(e.awsErrorDetails().errorMessage());
    }
}
    private static void deleteSQS(final SqsClient sqsClient, final String queueName) {
        try {
            final GetQueueUrlRequest getQueueRequest = (GetQueueUrlRequest)GetQueueUrlRequest.builder().queueName(queueName).build();
            final String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            final DeleteQueueRequest deleteQueueRequest = (DeleteQueueRequest)DeleteQueueRequest.builder().queueUrl(queueUrl).build();
            sqsClient.deleteQueue(deleteQueueRequest);
        }
        catch (SqsException e) {
            System.err.println("SQS ERROR:");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
    private static void deleteS3Bucket(final S3Client s3, final String[] pos) {
        final DeleteObjectRequest deleteObjectRequest = (DeleteObjectRequest)DeleteObjectRequest.builder().bucket(pos[0]).key(pos[1]).build();
        s3.deleteObject(deleteObjectRequest);
        final DeleteBucketRequest deleteBucketRequest = (DeleteBucketRequest)DeleteBucketRequest.builder().bucket(pos[0]).build();
        s3.deleteBucket(deleteBucketRequest);
    }


    public static void createWorker(final String tag) {
        try {
            System.out.println("Creating new worker");
//            final String script = "#!/bin/bash\njava -cp /Ass1/<jarName>.jar worker";
            String script = "#!/bin/sh\n"+
                    "rpm --import https://yum.corretto.aws/corretto.key\n"+
                    "curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo\n"+
                    "yum install -y java-15-amazon-corretto-devel\n"+
                    "aws s3 cp s3://"+jarsBucket+"/worker.jar /home/ec2-user\n"+
                    "cd /home/ec2-user\n"+
                    "java -jar worker.jar >> a.out";
            Manager.workers.add(createInstance.createInstance(Manager.ec2, tag, Manager.ami, script));
        }
        catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }




}







