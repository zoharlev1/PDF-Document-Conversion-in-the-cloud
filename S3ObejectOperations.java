//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//


import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObejectOperations {
    public S3ObejectOperations() {
    }

    public static String[] uploadFile(S3Client s3, String fileName) {
        Region region = Region.US_EAST_1;
        String bucket = "workers-bucket";
        createBucket(s3, bucket, region);
        String key = fileName ;
        s3.putObject((PutObjectRequest)PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(new File(fileName)));
        System.out.println("uploaded successfully");
        String[] uploadPossition = new String[]{bucket, key};
        return uploadPossition;
    }

    public static String[] uploadJson(S3Client s3, String json) {
        Region region = Region.US_EAST_1;
        s3 = (S3Client)((S3ClientBuilder)S3Client.builder().region(region)).build();
        String bucket = "bucket" + System.currentTimeMillis();
        createBucket(s3, bucket, region);
        String key = "json" + System.currentTimeMillis();
        s3.putObject((PutObjectRequest)PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromString(json));
        System.out.println("uploaded successfully");
        String[] uploadPossition = new String[]{bucket, key};
        return uploadPossition;
    }

    private static void createBucket(S3Client s3, String bucket, Region region) {
        s3.createBucket((CreateBucketRequest)CreateBucketRequest.builder().bucket(bucket).createBucketConfiguration((CreateBucketConfiguration)CreateBucketConfiguration.builder().build()).build());
        System.out.println(bucket);
    }

    public static JsonReader getBucketFile(S3Client s3, String[] pos) {
        GetObjectRequest getObjectRequest = (GetObjectRequest)GetObjectRequest.builder().bucket(pos[0]).key(pos[1]).build();
        ResponseInputStream response = null;

        try {
            response = s3.getObject(getObjectRequest);
        } catch (Exception var5) {
            System.err.println(var5.getMessage());
        }

        assert response != null;

        return new JsonReader(new InputStreamReader(response, StandardCharsets.UTF_8));
    }
}
