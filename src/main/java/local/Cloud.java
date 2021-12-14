package local;

import java.util.List;

import com.amazonaws.util.Base64;

import main.Config;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;

public class Cloud {

	private Ec2Client ec2;

	public Cloud() {
		ec2 = Ec2Client.builder().build();
	}

	public boolean isExist() {
		boolean exist = false;
		List<Reservation> reservationsList = ec2.describeInstances().reservations();
		CHECK: for (Reservation reservation : reservationsList) {
			List<Instance> instancesList = reservation.instances();
			for (Instance instance : instancesList) {
				for (Tag tag : instance.tags()) {
					if (tag.key().equals("Type") && tag.value().equals("Manager")
							&& isRunningOrPending(instance)) {
						exist = true;
						break CHECK;
					}
				}
			}
		}
		return exist;
	}

	private boolean isRunningOrPending(Instance ins) {
		return (ins.state().code() <= 16);
	}

	public void run() {
		String managerScript = Config.REMOTE_SCRIPT + "manager";
		RunInstancesRequest run_request = RunInstancesRequest.builder()
				.imageId(Config.AMAZON_LINUX_2_AMI_ID).instanceType(InstanceType.T2_MICRO)
				.userData(Base64.encodeAsString(managerScript.getBytes())).maxCount(1)
				.minCount(1).iamInstanceProfile(Config.ROLE).build();
		RunInstancesResponse run_response = ec2.runInstances(run_request);
		String instanceID = run_response.instances().get(0).instanceId();
		Tag tagName = Tag.builder().key("Name").value("Manager").build();
		Tag tagType = Tag.builder().key("Type").value("Manager").build();
		CreateTagsRequest tag_request = CreateTagsRequest.builder().resources(instanceID)
				.tags(tagName, tagType).build();
		ec2.createTags(tag_request);
		System.out.println("Successfully started EC2 instance " + instanceID
				+ " based on AMI " + Config.AMAZON_LINUX_2_AMI_ID);
	}

}
