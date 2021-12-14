package manager;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
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
import software.amazon.awssdk.services.ec2.model.TerminateInstancesRequest;

public class Manager {

	private static final int MAX_WORKERS = 8;

	static volatile boolean isRunning = true;
	private static ExecutorService localsPool, workersPool;
	static Ec2Client ec2;
	static AmazonSQS sqs;
	static AmazonS3 s3;
	static Map<String, List<Task>> tasksOfLocalApp;
	private static Queue<Message> seenMessages;

	public static void main(String[] args) {
		tasksOfLocalApp = new ConcurrentHashMap<String, List<Task>>();
		seenMessages = new ConcurrentLinkedQueue<Message>();
		ec2 = Ec2Client.builder().build();
		s3 = AmazonS3ClientBuilder.defaultClient();
		sqs = AmazonSQSClientBuilder.defaultClient();
		localsPool = Executors.newFixedThreadPool(MAX_WORKERS);
		workersPool = Executors.newFixedThreadPool(MAX_WORKERS);
		Thread locals = new Thread(() -> localAppListener(), "locals");
		Thread workers = new Thread(() -> workersListener(), "workers");
		locals.start();
		workers.start();
		try {
			locals.join();
			workers.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		terminate();
	}

	private static void workersListener() {
		while (isRunning || !tasksOfLocalApp.isEmpty()) {
			List<Callable<Object>> todo = new ArrayList<Callable<Object>>(MAX_WORKERS);
			for (int i = 0; i < MAX_WORKERS; i++) {
				todo.add(Executors.callable(() -> checkCompletedMessages()));
			}
			try {
				workersPool.invokeAll(todo);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			ArrayList<String> completedLocalApps = getCompletedLocalApps();
			if (!seenMessages.isEmpty())
				clearWorkerDoneQueue(sqs);
			if (!completedLocalApps.isEmpty()) {
				informLocalApps(completedLocalApps);
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {}
		}
		workersPool.shutdown();
		while (!workersPool.isTerminated())
			;
	}

	private static void localAppListener() {
		List<Message> messages = null;
		while (isRunning) {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
					Config.MANAGER_TASK_QUEUE_URL).withMaxNumberOfMessages(10)
							.withVisibilityTimeout(600);
			messages = sqs
					.receiveMessage(
							receiveMessageRequest.withMessageAttributeNames("All"))
					.getMessages();
			for (Message msg : messages) {
				localsPool.execute(new LocalAppHandler(msg));
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {}
		}
		localsPool.shutdown();
		while (!localsPool.isTerminated())
			;
	}

	private static List<Message> getCompletedMessages() {
		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
				Config.WORKER_DONE_QUEUE_URL).withMessageAttributeNames("All")
						.withVisibilityTimeout(600).withMaxNumberOfMessages(10);
		return sqs.receiveMessage(receiveMessageRequest).getMessages();
	}

	private static void checkCompletedMessages() {
		for (Message completionMessage : getCompletedMessages()) {
			Map<String, MessageAttributeValue> messageAttributes = completionMessage
					.getMessageAttributes();
			if (!messageAttributes.containsKey("Type") || !messageAttributes.get("Type")
					.getStringValue().equals("done PDF task"))
				continue;
			String localAppID = messageAttributes.get("localAppID").getStringValue();
			String op = messageAttributes.get("operation").getStringValue();
			String input = messageAttributes.get("originalURL").getStringValue();
			String output = messageAttributes.get("output").getStringValue();
			finishTask(localAppID, op, input, output);
			seenMessages.add(completionMessage);
		}
	}

	private static ArrayList<String> getCompletedLocalApps() {
		ArrayList<String> finishedLocalApps = new ArrayList<String>();
		for (String key : tasksOfLocalApp.keySet()) {
			if (isLocalDone(key)) {
				finishedLocalApps.add(key);
			}
		}
		return finishedLocalApps;
	}

	static void clearAppManagerSQS(AmazonSQS sqs, Message message) {
		DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(
				Config.MANAGER_TASK_QUEUE_URL, message.getReceiptHandle());
		sqs.deleteMessage(deleteMessageRequest);
	}

	private static void clearWorkerDoneQueue(AmazonSQS sqs) {
		for (Message message : seenMessages) {
			DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(
					Config.WORKER_DONE_QUEUE_URL, message.getReceiptHandle());
			sqs.deleteMessage(deleteMessageRequest);
		}
	}

	static synchronized void openWorkers(int amount) {
		if (amount > MAX_WORKERS)
			amount = MAX_WORKERS;
		int runningWorkers = getNumOfWorkers();
		int workersToOpen = amount - runningWorkers;
		for (int i = 0; i < workersToOpen; i++) {
			String workerID = String.valueOf(runningWorkers + i);
			String userData = Config.REMOTE_SCRIPT + workerID;
			RunInstancesRequest runRequest = RunInstancesRequest.builder()
					.imageId(Config.AMAZON_LINUX_2_AMI_ID)
					.instanceType(InstanceType.T2_MICRO)
					.userData(Base64.encodeAsString(userData.getBytes())).maxCount(1)
					.minCount(1).iamInstanceProfile(Config.ROLE).build();
			RunInstancesResponse run_response = ec2.runInstances(runRequest);
			String instanceID = run_response.instances().get(0).instanceId();
			Tag tagName = Tag.builder().key("Name").value("Worker").build();
			Tag tagType = Tag.builder().key("Type").value("Worker").build();
			try {
				// wait to make sure the instance request has gone through
				Thread.sleep(1000);
				CreateTagsRequest tag_request = CreateTagsRequest.builder()
						.resources(instanceID).tags(tagName, tagType).build();
				ec2.createTags(tag_request);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("a new worker was created");
		}
	}

	private static int getNumOfWorkers() {
		List<Reservation> reservationsList = ec2.describeInstances().reservations();
		int numOfinstances = -1; // not including manager
		for (Reservation reservation : reservationsList) {
			List<Instance> instancesList = reservation.instances();
			for (Instance instance : instancesList) {
				if (instance.state().code() <= 16) // running or pending
					numOfinstances++;
			}
		}
		return numOfinstances;
	}

	public static boolean isTerminationMessage(
			Map<String, MessageAttributeValue> messageAttributes) {
		return messageAttributes.containsKey("Type")
				&& messageAttributes.get("Type").getStringValue().equals("terminate");
	}

	private static void informLocalApps(ArrayList<String> finishedLocals) {
		for (String local : finishedLocals) {
			String sumfile = createSummaryFile(local);
			SendMessageRequest msgRequest = new SendMessageRequest(
					Config.MANAGER_DONE_QUEUE_URL, "done task");
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("Type", new MessageAttributeValue()
					.withDataType("String").withStringValue("done task"));
			messageAttributes.put("URL", new MessageAttributeValue()
					.withDataType("String").withStringValue(sumfile));
			messageAttributes.put("localAppID", new MessageAttributeValue()
					.withDataType("String").withStringValue(local));
			msgRequest.withMessageAttributes(messageAttributes);
			sqs.sendMessage(msgRequest);
			tasksOfLocalApp.remove(local);
		}
	}

	private static void finishTask(String id, String op, String input, String output) {
		Task finished = new Task(op, input, output);
		for (Task t : tasksOfLocalApp.get(id)) {
			if (t.equals(finished)) {
				t.setOutput(output);
				return;
			}
		}
	}

	private static boolean isLocalDone(String id) {
		for (Task t : tasksOfLocalApp.get(id)) {
			if (!t.isFinished())
				return false;
		}
		return true;
	}

	private static String createSummaryFile(String local) {
		PrintWriter out = null;
		try {
			File f = new File("summaryFile_" + local + ".txt");
			out = new PrintWriter(f);
			List<Task> tasks = tasksOfLocalApp.get(local);
			for (Task t : tasks)
				out.write(t.toString() + "\n");
			out.flush();
			String key = f.getName().replace('\\', '_').replace('/', '_').replace(':',
					'_');
			PutObjectRequest req = new PutObjectRequest(
					Config.CONVERTED_FILES_BUCKET_NAME, key, f)
							.withCannedAcl(CannedAccessControlList.PublicRead);
			s3.putObject(req);
			f.delete();
			return f.getName();
		} catch (IOException e) {
			e.printStackTrace();
			return "error creating summary file";
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	private static void terminate() {
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

	public static void deleteBucket(String bucketName) {
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
