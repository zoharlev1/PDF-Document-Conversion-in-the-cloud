import org.apache.commons.io.FilenameUtils;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.io.RandomAccessFile;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import org.fit.pdfdom.PDFDomTree;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.imageio.ImageIO;
import javax.xml.parsers.ParserConfigurationException;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Worker {

    static SqsClient sqsWorker = SqsClient.builder().region(Region.US_EAST_1).build();
    static GetQueueUrlResponse getQueueUrlResponseOutPut =
            sqsWorker.getQueueUrl(GetQueueUrlRequest.builder().queueName("From_workers").build());
    static GetQueueUrlResponse getQueueUrlResponseInPut =
            sqsWorker.getQueueUrl(GetQueueUrlRequest.builder().queueName("To_workers").build());

    private static final String sqsInPut = getQueueUrlResponseInPut.queueUrl();
    private  static final String sqsOutPut = getQueueUrlResponseOutPut.queueUrl();


    static S3Client s3 = ((S3ClientBuilder)S3Client.builder().region(Region.US_EAST_1)).build();



    public static void main(String[] args) throws Exception {

        while (true) {
            try {
                ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder().queueUrl(sqsInPut)
                        .messageAttributeNames("id", "action", "url_to_operate")
                        .maxNumberOfMessages(1)
                        .visibilityTimeout(30)
                        .build();
                List<Message> messageList = sqsWorker.receiveMessage(receiveRequest).messages();

                if(messageList==null || messageList.size()==0)
                    continue;

                Message message = messageList.get(0);

                    //termination case
                    if (message.body().startsWith("terminate\t")) {
                        sqsWorker.close();
                    } else {
                        //first to delete the msg from the queue and than create the PDFjob
                        //processing the request & sending it
                        sendResults(message);
                        //the erasing process
                        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                                .receiptHandle(message.receiptHandle())
                                .queueUrl(sqsInPut)
                                .build();
                        sqsWorker.deleteMessage(deleteRequest);
                    }
            } catch (SqsException e) {
                System.err.println(e.awsErrorDetails().errorMessage());
            }
        }
    }


    private static void sendResults(Message job) throws IOException {
        String[] split = job.body().split("\t");
        String id = split[0];
        String operation = split[1];
        String URL = split[2];
        String[] result = null;

        //downloading

        URL url = new URL(URL);
        URLConnection urlConn = url.openConnection();
        String file_name = FilenameUtils.getBaseName(url.getPath());

        // Checking whether the URL contains a PDF
        if (urlConn==null){
            sendErrorMsg("ERROR- not a pdf", operation, URL, id);
        }
        else {
            try (InputStream in = url.openStream()) {
                //saving the pdf from the url
                Files.copy(in, Paths.get(file_name + ".pdf"), StandardCopyOption.REPLACE_EXISTING);
                String path = "./" + file_name + ".pdf";

                // we have a valid pdf - right now we need to operate and send back the results
                switch (operation) {
                    case "ToImage":
                        result = toPNG(path, file_name);
                        break;
                    case "ToHTML":
                        result = toHTML(path, file_name);
                        break;
                    case "ToText":
                        result = toTXT(path, file_name);
                        break;
                }
                    // sending the results to the manager
                try {
                    SendMessageRequest send_msg_request = SendMessageRequest.builder()
                            .queueUrl(sqsOutPut)
                            .messageBody(id +"\t" + result[0] + "\t"+ operation + "\t" + URL +"\t"+result[1])
                            .delaySeconds(5)
                            .build();
                    sqsWorker.sendMessage(send_msg_request);
                } catch (SqsException e) {
                    sendErrorMsg("ERROR- failed sending", operation, URL, id);
                }

            } catch (IOException | ParserConfigurationException e) {
                sendErrorMsg("ERROR- invalid URL", operation, URL, id);
            }
        }
    }

    // the string should indicate if there is any problem- otherwise- the url
    // first- download the pdf
    // secondly- operate the desired action
    // upload the resulting file to S3
    // send the result string
    private static String[] toPNG(String path, String file_name) throws IOException {
        //creating the text file
        String URLout = "";
        File file = new File(path);
        //starting the conversion
        PDDocument pdDoc = PDDocument.load(file);
        PDFRenderer renderer = new PDFRenderer(pdDoc);
        BufferedImage image = renderer.renderImageWithDPI(0, 300, ImageType.RGB);
        File img = new File("./" + file_name + ".png");
        ImageIO.write(image, "PNG", img);
        pdDoc.close();
        //now we need to update to S3
        String[] bucketKey=S3ObejectOperations.uploadFile(Worker.s3,file_name+ ".png");
        //deleting the local file
        img.delete();
        file.delete();
        return bucketKey;
    }

    private static String[] toHTML(String path, String file_name) throws IOException, ParserConfigurationException {
        //creating the text file
        File file = new File(path);
        File HTML = new File("./"+ file_name + ".html");
        //starting the conversion
        PDDocument pdDoc = PDDocument.load(file);
        PDFText2HTML temp = new PDFText2HTML();
        String html = temp.getText(pdDoc);
        FileWriter fwriter = new FileWriter(HTML.getName());
        Writer writer = new BufferedWriter(fwriter);
        writer.write(html);

        //upload to S3
        String[] bucketKey=S3ObejectOperations.uploadFile(Worker.s3,file_name + ".html");

        pdDoc.close();
        fwriter.close();
        writer.close();
        //HTML.delete();
        return bucketKey;
    }



    private static String[] toTXT(String path, String file_name) throws IOException {

        //creating the text file
        File file = new File(path);
        //starting the conversion
        PDFParser parser = new PDFParser(new RandomAccessFile(file, "r"));
        parser.parse();
        COSDocument cosDoc = parser.getDocument();
        PDDocument pdDoc = new PDDocument(cosDoc);
        //Instantiate PDFTextStripper class
        PDFTextStripper pdfStripper = new PDFTextStripper();
        //Retrieving text from PDF document
        String text = pdfStripper.getText(pdDoc);

        File txt = new File("./" + file_name + ".txt");
        PrintWriter pwriter = new PrintWriter(txt.getName());
        pwriter.print(text);
        Files.write(Paths.get(file_name + ".txt"), text.getBytes());
        pdDoc.close();

        //upload to S3
        String[] bucketKey=S3ObejectOperations.uploadFile(Worker.s3,file_name+ ".txt");

        //deleting the local file& closing files
        if (cosDoc != null)
            cosDoc.close();

        if (pdDoc != null)
            pdDoc.close();

        pwriter.close();
        //txt.delete();
        // need to change it to s3 location
        return bucketKey;
    }


    public static void sendErrorMsg(String ErrorMsg, String operation, String URL, String id){
        try {
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(sqsOutPut)
                    .messageBody(id +"\t" + ErrorMsg + "\t"+ operation + "\t" + URL + "\t" + "old URL: " + URL)
                    .delaySeconds(5)
                    .build();
            sqsWorker.sendMessage(send_msg_request);
        } catch (SqsException e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }
}

