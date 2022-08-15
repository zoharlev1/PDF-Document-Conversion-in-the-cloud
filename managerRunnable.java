import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.regions.Regions.US_EAST_1;

public class managerRunnable implements Runnable {

    private Message message;
    private final AmazonS3 amazonS3Client = (AmazonS3) AmazonS3ClientBuilder.standard().withRegion(US_EAST_1).build();
    public static AtomicInteger missions;
    public static AtomicBoolean input;
    public static AtomicBoolean mdone;
    public static String[] workerbucket;
    private static ConcurrentHashMap<Integer, Message> messagesById;
    private static AtomicInteger messageId;


    public managerRunnable(Message m) {
        this.message = m;
        missions = new AtomicInteger(0);
        input = new AtomicBoolean(false);
        mdone = new AtomicBoolean(false);
        workerbucket = new String[2];
        messagesById = new ConcurrentHashMap<Integer, Message>();
        messageId = new AtomicInteger(0);
    }

    @Override
    public void run() {
        //first part- get the job from s3
        //in the map i have the sqs url & bucket name and key
        try {
            String[] split;
            split = message.body().split("\t");
            String bucketName = split[1];
            String keyName = split[2];
            S3Object s3Object = this.amazonS3Client.getObject(bucketName, keyName);
            InputStreamReader streamReader;
            streamReader = new InputStreamReader(s3Object.getObjectContent(), StandardCharsets.UTF_8);
            BufferedReader reader = new BufferedReader(streamReader);
            String st = "";
            //second- create the splits
            //lock.lock();
            while ((st = reader.readLine()) != null) {
                String[] ms = st.split("\t");
                String action = ms[0];
                String URL = ms[1];
                //third- send the splits to the workers
                SendMessageRequest send = (SendMessageRequest) SendMessageRequest.builder()
                        .queueUrl(Manager.toWorkers)
                        .messageBody(messageId + "\t" + action + "\t" + URL)
                        .build();
                Manager.sqs.sendMessage(send);
                messageId.getAndIncrement();
                missions.incrementAndGet();
            }
            this.input.set(true);
            //lock.unlock();
            //delete the work- we're already taking care of it
            final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(Manager.URLfromApp)
                    .receiptHandle(message.receiptHandle()).build();
            Manager.sqs.deleteMessage(deleteMessageRequest);

            //check if we need extra hands
            if (missions.get() > 0)
                manageWorkers();
            //fourth- and last - the reduce part, we are preparing the output
            recieveMessages();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }


    }


    public void recieveMessages() throws InterruptedException, IOException {
        while (!mdone.get()) {
            Thread.sleep(2000L);
            List<Message> responses = Manager.sqs.receiveMessage(ReceiveMessageRequest
                    .builder().queueUrl(Manager.fromWorkers)
                    .visibilityTimeout(30).build()).messages();
            if (responses.size() != 0) {
                for (Message m : responses) {
                    String[] split = m.body().split("\t");
                    if (!messagesById.containsKey(Integer.valueOf(split[0]))) {
                        messagesById.put(Integer.valueOf(split[0]), m);
                    }
                    if (missions.get() > 0) {
                        missions.decrementAndGet();
                    }
                }
            }
            //sent all the tasks and got all the responses
            if (input.get() && missions.get() == 0) {
                mdone.set(true);
                uploadSummery();
            }
        }
    }

    public void uploadSummery() throws InterruptedException, IOException {
        String summary = "";
        final String URLToApp = Manager.sqs.getQueueUrl(GetQueueUrlRequest.builder().queueName("Manager_To_App").build()).queueUrl();
        System.out.println("Starting to summaries from manager");
        BufferedWriter writer = new BufferedWriter(new FileWriter("outFile.txt", true));
        for (int i = 0; i < messagesById.size(); i++) {
            final Message msg = messagesById.get(i);
            if (msg != null) {
                String[] split = msg.body().split("\t");
                summary = split[2] + " " + split[3] + " " + split[1] + " " + split[4] + "\n";
                writer.append(summary);
            }
        }
        writer.close();
        workerbucket = S3ObejectOperations.uploadFile(Manager.s3, "outFile.txt");
        System.out.printf("File was uploaded to address : bucket - %s, key - %s \n", workerbucket[0], workerbucket[1]);
        final SendMessageRequest sendMessageRequest = SendMessageRequest.builder().queueUrl(URLToApp)
                .messageBody("summary\t" + workerbucket[0] + "\t" + workerbucket[1])
                .build();
        Manager.sqs.sendMessage(sendMessageRequest);
        for (int j = 0; j < messagesById.size(); ++j) {
            if(messagesById.get(j) != null){
                final DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder().queueUrl(Manager.fromWorkers).receiptHandle(messagesById.get(j).receiptHandle()).build();
                Manager.sqs.deleteMessage(deleteMessageRequest);
            }
        }
    }


    public static void manageWorkers() {
        if (Manager.workers.size() == 0 || (missions.get() / Manager.workers.size()) > Manager.n) {
            if (Manager.workers.size() == 0) {
                Manager.createWorker("worker" + Manager.workersNumber.incrementAndGet());
                Manager.workersNumber.set(1);
            } else {
                int requiredWorkers = (missions.get() / Manager.workers.size()) - Manager.n;
                if (requiredWorkers + Manager.workers.size() > Manager.maxWorkers) {
                    requiredWorkers = Manager.maxWorkers - Manager.workers.size();
                }
                for (int i = 0; i < requiredWorkers; i++) {
                    Manager.createWorker("worker" + Manager.workersNumber.incrementAndGet());
                }
                Manager.workersNumber.set(Manager.workers.size());
//            Manager.workersLock.unlock();
            }
        }
    }
}


