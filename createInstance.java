//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;

public class createInstance {
    public createInstance() {
    }

    public static String createInstance(Ec2Client ec2, String name, String amiId, String command) {
        IamInstanceProfileSpecification role = (IamInstanceProfileSpecification)IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build();
        String LongCommand = "#!/bin/bash\nwget https://jaryoload.s3.amazonaws.com/Assignment1.jar \n" + command;
        String base64UserData = new String(Base64.getEncoder().encode(LongCommand.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        RunInstancesRequest runRequest = (RunInstancesRequest)RunInstancesRequest.builder().instanceType(InstanceType.T2_MICRO).imageId(amiId).maxCount(1).minCount(1).userData(base64UserData).iamInstanceProfile(role).build();
        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = ((Instance)response.instances().get(0)).instanceId();
        Tag tag = (Tag)Tag.builder().key("name").value(name).build();
        CreateTagsRequest tagRequest = (CreateTagsRequest)CreateTagsRequest.builder().resources(new String[]{instanceId}).tags(new Tag[]{tag}).build();

        try {
            ec2.createTags(tagRequest);
            System.out.printf("Successfully started EC2 instance %s based on AMI %s, Tag value %s\n", instanceId, amiId, tag.value());
        } catch (Ec2Exception var13) {
            System.err.println(var13.getMessage());
            System.exit(1);
        }

        return instanceId;
    }
}
