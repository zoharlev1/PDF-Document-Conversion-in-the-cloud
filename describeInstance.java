//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//



import java.util.Iterator;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesRequest;
import software.amazon.awssdk.services.ec2.model.DescribeInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.Tag;

public class describeInstance {
    public describeInstance() {
    }

    public static Boolean describeEC2Instances(Ec2Client ec2) {
        String nextToken = null;
        boolean flag = false;

        try {
            do {
                DescribeInstancesRequest request = (DescribeInstancesRequest)DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                Iterator var5 = response.reservations().iterator();

                label40:
                while(var5.hasNext()) {
                    Reservation reservation = (Reservation)var5.next();
                    Iterator var7 = reservation.instances().iterator();

                    while(true) {
                        while(true) {
                            if (!var7.hasNext()) {
                                continue label40;
                            }

                            Instance instance = (Instance)var7.next();
                            Iterator var9 = instance.tags().iterator();

                            while(var9.hasNext()) {
                                Tag t = (Tag)var9.next();
                                if (t.value().equals("Manager") && !instance.state().name().toString().equals("terminated")) {
                                    flag = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                nextToken = response.nextToken();
            } while(nextToken != null);
        } catch (Ec2Exception var11) {
            System.err.println(var11.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return flag;
    }

    public static String getInstanceID(Ec2Client ec2) {
        String nextToken = null;
        String id = "";

        try {
            do {
                DescribeInstancesRequest request = (DescribeInstancesRequest)DescribeInstancesRequest.builder().maxResults(6).nextToken(nextToken).build();
                DescribeInstancesResponse response = ec2.describeInstances(request);
                Iterator var5 = response.reservations().iterator();

                label40:
                while(var5.hasNext()) {
                    Reservation reservation = (Reservation)var5.next();
                    Iterator var7 = reservation.instances().iterator();

                    while(true) {
                        while(true) {
                            if (!var7.hasNext()) {
                                continue label40;
                            }

                            Instance instance = (Instance)var7.next();
                            Iterator var9 = instance.tags().iterator();

                            while(var9.hasNext()) {
                                Tag t = (Tag)var9.next();
                                if (t.value().equals("Manager") && !instance.state().name().toString().equals("terminated")) {
                                    id = instance.instanceId();
                                    break;
                                }
                            }
                        }
                    }
                }

                nextToken = response.nextToken();
            } while(nextToken != null);
        } catch (Ec2Exception var11) {
            System.err.println(var11.awsErrorDetails().errorMessage());
            System.exit(1);
        }

        return id;
    }
}
