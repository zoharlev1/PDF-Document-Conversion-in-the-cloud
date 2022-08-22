import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.Ec2ClientBuilder;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.InstanceStateChange;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.amazonaws.regions.Regions.US_EAST_1;


public class LocalApplication
{
    static SqsClient sqs;
    static Ec2Client ec2;
    static S3Client s3;
    private static final AmazonS3 amazonS3Client;
    final static String managerToAppSQS = "Manager_To_App";
    final static String appToManagerSQS = "App_To_Manager";

    public static void main(final String[] args) throws IOException {

        System.out.println("Local Application Start");
        boolean terminate = false;


        if (args.length == 4) {
            terminate = args[3].equals("termination");
            if (!terminate) {
                System.out.println("Invalid terminate argument! expected: \"terminate\"  actual: \"" + args[3] + "\"");
                return;
            }
        }
        if (args.length < 3) {
            System.out.println("Invalid arguments number! need at least 3 arguments- got " + args.length);
            return;
        }
        final String n = args[2];
        if (Integer.parseInt(n) <= 0) {
            System.out.println("Invalid worker's ratio n, should be > 0");
            return;
        }
        File out =  new File(args[1]);
        if(!out.exists())
            out.createNewFile();
        try {
            final File in = new File(args[0]);
            if (!in.exists()) {
                System.out.println(args[0]);
                System.out.println("Invalid file!");
                return;
            }
        } catch (Exception e5) {
            System.out.println("Invalid files!");
        }


        // manager init
        final boolean existsManager = describeInstance.describeEC2Instances(LocalApplication.ec2);
        System.out.println("Is there a manager running? " + existsManager);
        String managerInstance = "";
        if (!existsManager) {
            final String ami = "ami-00e95a9222311e8ed";
            final String cmd =  "#!/bin/sh\n"+
                    "rpm --import https://yum.corretto.aws/corretto.key\n"+
                    "curl -L -o /etc/yum.repos.d/corretto.repo https://yum.corretto.aws/corretto.repo\n"+
                    "yum install -y java-15-amazon-corretto-devel\n"+
                    "aws s3 cp s3://"+"https://myappjarbucket.s3.amazonaws.com/Manager1.jar"+"/worker.jar /home/ec2-user\n"+
                    "cd /home/ec2-user\n"+
                    "java -jar worker.jar" +ami + " " + n + " " + args[1] + ">> a.out";
            managerInstance = createInstance.createInstance(LocalApplication.ec2, "Manager", ami, cmd);
        } else {
            managerInstance = describeInstance.getInstanceID(LocalApplication.ec2);
        }

        //upload the file to S3 - & building the sqs queues'
        LocalApplication.sqs.createQueue(CreateQueueRequest.builder().queueName(appToManagerSQS).build());
        LocalApplication.sqs.createQueue(CreateQueueRequest.builder().queueName(managerToAppSQS).build());
        final String URLAppToM = LocalApplication.sqs.getQueueUrl((GetQueueUrlRequest) GetQueueUrlRequest.builder().queueName(appToManagerSQS).build()).queueUrl();
        final String URLFromM = LocalApplication.sqs.getQueueUrl((GetQueueUrlRequest) GetQueueUrlRequest.builder().queueName(managerToAppSQS).build()).queueUrl();
        final String fileName = args[0];
        final String[] BucketKey = S3ObjectOperations.uploadFile(LocalApplication.s3, fileName);
        System.out.printf("File was uploaded to address : bucket - %s, key - %s \n", BucketKey[0], BucketKey[1]);
        LocalApplication.sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(URLAppToM)
                .messageBody("job\t" + BucketKey[0] + "\t" + BucketKey[1] + "\t" +URLFromM)
                .build());
        //deleting the local file
        Files.deleteIfExists(Paths.get(args[1]));
        System.out.println("Waiting to receive messages...");

        // accepting and dealing with the end of the manager's job
        while (true) {
            try {
                List<Message> messages = LocalApplication.sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(URLFromM)
                        .visibilityTimeout(Integer.valueOf(10)).build()).messages();
                if (messages.size() != 0) {
                    String[] split = messages.get(0).body().split("\t");
                    if (split[0].equals("summary")) {
                        System.out.println("converting to HTML file");
                        generateHTML(split[1], split[2],out.getName());
                        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(URLFromM).receiptHandle(messages.get(0).receiptHandle()).build();
                        LocalApplication.sqs.deleteMessage(deleteMessageRequest);
                        System.out.println("Deleting bucket " + split[1]);
                        deleteS3Bucket(LocalApplication.s3, new String[]{split[1], split[2]});

                if (terminate) {
                    System.out.println("starting termination sequence local");
                    final SendMessageRequest sendMsg = (SendMessageRequest) SendMessageRequest.builder()
                            .queueUrl(URLAppToM)
                            .messageBody("terminate\t" + URLAppToM)
                            .build();
                    LocalApplication.sqs.sendMessage(sendMsg);
                    boolean ack = false;
                   while (!ack) {
                        final ReceiveMessageRequest request3 = (ReceiveMessageRequest) ReceiveMessageRequest.builder().queueUrl(URLFromM)
                        .build();
                        final List<Message> messages2 = (List<Message>) LocalApplication.sqs.receiveMessage(request3).messages();
                        for (final Message i : messages2) {
                            String[] split1 = i.body().split("\t");
                            if (split1[0].equals("terminated")) {
                                ack = true;
                                System.out.println("Received termination acknowledge from manager");
                            }
                        }
                    }
                    terminateEC2(LocalApplication.ec2, Collections.singletonList(managerInstance));
                }
                deleteSQS(LocalApplication.sqs, managerToAppSQS);
                deleteSQS(LocalApplication.sqs, appToManagerSQS);
                System.out.println("Deleting bucket " + BucketKey[0]);
                deleteS3Bucket(LocalApplication.s3, BucketKey);
                LocalApplication.ec2.close();
                LocalApplication.s3.close();
                LocalApplication.sqs.close();
                System.out.println("Local Application Finished Successfully. Good Bye");
                System.exit(0);
            }
                }

            }
            catch(SdkClientException e){
            }
        }
    }


    private static void generateHTML(String bucket, String key,String outputFileName) throws IOException {
        String summary;
        ResponseInputStream stream = s3.getObject(GetObjectRequest.builder().key(key).bucket(bucket).build());
        BufferedReader buffer = new BufferedReader(new InputStreamReader(stream));
        FileWriter file_writer = new FileWriter(outputFileName+".html");
        Writer writer = new BufferedWriter(file_writer);
        writer.write("<html><head><title>Summary file: </title></head>\n");
        writer.write("<body>\n");
        while ((summary = buffer.readLine()) != null) {
            writer.write("<p>" + summary + "</p>");
            writer.write('\n');
        }
        writer.write("</body></html>");
        writer.close();
        file_writer.close();

    }

    private static void deleteS3Bucket(final S3Client s3, final String[] pos) {
        final DeleteObjectRequest deleteObjectRequest = (DeleteObjectRequest)DeleteObjectRequest.builder().bucket(pos[0]).key(pos[1]).build();
        s3.deleteObject(deleteObjectRequest);
        final DeleteBucketRequest deleteBucketRequest = (DeleteBucketRequest)DeleteBucketRequest.builder().bucket(pos[0]).build();
        s3.deleteBucket(deleteBucketRequest);
    }

    public static void terminateEC2(final Ec2Client ec2, final Collection<String> instanceID) {
        try {
            final TerminateInstancesRequest ti = (TerminateInstancesRequest)TerminateInstancesRequest.builder().instanceIds((Collection)instanceID).build();
            final TerminateInstancesResponse response = ec2.terminateInstances(ti);
            final List<InstanceStateChange> list = (List<InstanceStateChange>)response.terminatingInstances();
            for (final InstanceStateChange sc : list) {
                System.out.println("The ID of the terminated instance is " + sc.instanceId());
            }
        }
        catch (Ec2Exception e) {
            System.err.println("EC2 ERROR:");
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
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

    static {
        LocalApplication.sqs = (SqsClient)((SqsClientBuilder)SqsClient.builder().region(Region.US_EAST_1)).build();
        LocalApplication.ec2 = (Ec2Client)((Ec2ClientBuilder)Ec2Client.builder().region(Region.US_EAST_1)).build();
        LocalApplication.s3 = (S3Client)((S3ClientBuilder)S3Client.builder().region(Region.US_EAST_1)).build();
        amazonS3Client = (AmazonS3)AmazonS3ClientBuilder.standard().withRegion(US_EAST_1).build();
    }
}
