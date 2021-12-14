package local;

import java.util.Iterator;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;

import main.Config;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Instance;
import software.amazon.awssdk.services.ec2.model.Reservation;
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;

public class Reset {

	private static AmazonS3 s3;

	public static void main(String[] args) {
		terminate();
	}

	private static void terminate() {
		AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();
		s3 = AmazonS3ClientBuilder.defaultClient();
		Ec2Client ec2 = Ec2Client.builder().build();
		System.out.println("closing s3");
		deleteBucket(Config.PENDING_FILES_BUCKET_NAME);
		deleteBucket(Config.CONVERTED_FILES_BUCKET_NAME);
		deleteBucket(Config.JAR_BUCKET_NAME);
		System.out.println("closing sqs");
		String[] qs = { Config.WORKER_TASK_QUEUE_URL, Config.MANAGER_TASK_QUEUE_URL,
				Config.WORKER_DONE_QUEUE_URL, Config.MANAGER_DONE_QUEUE_URL };
		for (int i = 0; i < qs.length; i++)
			try {
				sqs.deleteQueue(qs[i]);
			} catch (Exception e) {}
		System.out.println("closing ec2");
		for (Reservation reservation : ec2.describeInstances().reservations()) {
			List<Instance> instancesList = reservation.instances();
			for (Instance instance : instancesList) {
				TerminateInstancesRequest terminateRequest = TerminateInstancesRequest
						.builder().instanceIds(instance.instanceId()).build();
				ec2.terminateInstances(terminateRequest);
			}
		}
	}

	private static void deleteBucket(String bucketName) {
		try {
			ObjectListing objectListing = s3.listObjects(bucketName);
			while (true) {
				Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries()
						.iterator();
				while (objIter.hasNext()) {
					s3.deleteObject(bucketName, objIter.next().getKey());
				}
				if (objectListing.isTruncated()) {
					objectListing = s3.listNextBatchOfObjects(objectListing);
				}
				else {
					break;
				}
			}
			s3.deleteBucket(bucketName);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (SdkClientException e) {
			e.printStackTrace();
		}
	}

}
