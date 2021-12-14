package main;

import java.util.UUID;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;

import software.amazon.awssdk.services.ec2.model.IamInstanceProfileSpecification;

public class Config {

	private static final String PREFIX = "talondspsassignment1";

	public static final IamInstanceProfileSpecification ROLE = IamInstanceProfileSpecification
			.builder().name("LabInstanceProfile").build();
	
	public static final String AMAZON_LINUX_2_AMI_ID = "ami-00e95a9222311e8ed";
	public static final String LOCAL_ID = UUID.randomUUID().toString();
	public static final String JAR_NAME = "remote.jar";
	public static final String JAR_BUCKET_NAME = PREFIX + "jarbucket";
	public static final String PENDING_FILES_BUCKET_NAME = PREFIX + "pendingbucket";
	public static final String CONVERTED_FILES_BUCKET_NAME = PREFIX + "convertedbucket";
	public static final String MANAGER_TASK_QUEUE_URL = PREFIX + "MANAGER_TASK_QUEUE_URL";
	public static final String MANAGER_DONE_QUEUE_URL = PREFIX + "MANAGER_DONE_QUEUE_URL";
	public static final String WORKER_TASK_QUEUE_URL = PREFIX + "WORKER_TASK_QUEUE_URL";
	public static final String WORKER_DONE_QUEUE_URL = PREFIX + "WORKER_DONE_QUEUE_URL";
	public static final String REMOTE_SCRIPT = "#!/bin/bash\r\n"
			+ "sudo yum update -y\r\n" + "sudo aws s3 cp s3://" + JAR_BUCKET_NAME + "/"
			+ JAR_NAME + " " + JAR_NAME + "\r\n" + "java -jar " + JAR_NAME + " ";

	String unused = "--region us-east-1";

	public static int tasksPerWorker = 50;
	public static boolean shouldTerminate = true;
	public static String inputFileName = "input.txt";
	public static String outputFileName = "output.html";

	public static void handleException(Exception e) {
		if (e instanceof AmazonServiceException) {
			AmazonServiceException ase = (AmazonServiceException) e;
			System.out.println(
					"Caught an AmazonServiceException, which means your request made it "
							+ "to Amazon S3, but was rejected with an error response for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		}
		else if (e instanceof AmazonClientException) {
			System.out.println(
					"Caught an AmazonClientException, which means the client encountered "
							+ "a serious internal problem while trying to communicate with S3, "
							+ "such as not being able to access the network.");
		}
		e.printStackTrace();
	}

}
