//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueNameExistsException;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class sendReceiveMessages {
    private static final String QUEUE_NAME = "testQueue" + (new Date()).getTime();

    public sendReceiveMessages() {
    }

    public static void main(String[] args) {
        SqsClient sqs = (SqsClient)((SqsClientBuilder)SqsClient.builder().region(Region.US_EAST_1)).build();

        try {
            CreateQueueRequest request = (CreateQueueRequest)CreateQueueRequest.builder().queueName(QUEUE_NAME).build();
            sqs.createQueue(request);
        } catch (QueueNameExistsException var10) {
            throw var10;
        }

        GetQueueUrlRequest getQueueRequest = (GetQueueUrlRequest)GetQueueUrlRequest.builder().queueName(QUEUE_NAME).build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        SendMessageRequest send_msg_request = (SendMessageRequest)SendMessageRequest.builder().queueUrl(queueUrl).messageBody("hello world").delaySeconds(5).build();
        sqs.sendMessage(send_msg_request);
        ReceiveMessageRequest receiveRequest = (ReceiveMessageRequest)ReceiveMessageRequest.builder().queueUrl(queueUrl).build();
        List<Message> messages = sqs.receiveMessage(receiveRequest).messages();
        Iterator var7 = messages.iterator();

        while(var7.hasNext()) {
            Message m = (Message)var7.next();
            DeleteMessageRequest deleteRequest = (DeleteMessageRequest)DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(m.receiptHandle()).build();
            sqs.deleteMessage(deleteRequest);
        }

    }
}
