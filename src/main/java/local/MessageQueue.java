package local;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import main.Config;

public class MessageQueue {

	AmazonSQS sqs;

	public MessageQueue() {
		sqs = AmazonSQSClientBuilder.standard().build();
		createAllQueues();
	}

	public boolean checkQueue(String name) {
		return sqs.listQueues().getQueueUrls().contains(name);
	}

	public void createQueue(String name) {
		if (checkQueue(name)) {
			System.out.println(name + " already exists!\n");
			return;
		}
		System.out.println("Creating a new SQS queue called " + name);
		CreateQueueRequest appToManagerQueueRequest = new CreateQueueRequest(name);
		sqs.createQueue(appToManagerQueueRequest);
		System.out.println(name + " was created");
	}

	public void createAllQueues() {
		createQueue(Config.MANAGER_TASK_QUEUE_URL);
		createQueue(Config.MANAGER_DONE_QUEUE_URL);
		createQueue(Config.WORKER_TASK_QUEUE_URL);
		createQueue(Config.WORKER_DONE_QUEUE_URL);
	}

	public void sendTaskMessageToManager(String key) {
		try {
			System.out.println(
					"Sending a message to " + Config.MANAGER_TASK_QUEUE_URL.toString());
			final Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("Type", new MessageAttributeValue()
					.withDataType("String").withStringValue("PDF task"));
			messageAttributes.put("localAppID", new MessageAttributeValue()
					.withDataType("String").withStringValue(Config.LOCAL_ID));
			messageAttributes.put("BucketName",
					new MessageAttributeValue().withDataType("String")
							.withStringValue(Config.PENDING_FILES_BUCKET_NAME));
			messageAttributes.put("FileName", new MessageAttributeValue()
					.withDataType("String").withStringValue(key));
			messageAttributes.put("tasksPerWorker",
					new MessageAttributeValue().withDataType("String")
							.withStringValue(String.valueOf(Config.tasksPerWorker)));
			final SendMessageRequest sendMessageRequest = new SendMessageRequest();
			sendMessageRequest.withMessageBody("task file");
			sendMessageRequest.withQueueUrl(Config.MANAGER_TASK_QUEUE_URL);
			sendMessageRequest.withMessageAttributes(messageAttributes);
			sqs.sendMessage(sendMessageRequest);
		} catch (AmazonClientException ace) {
			Config.handleException(ace);
		}
	}

	public void sendTerminateMsgToManager() {
		final Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
		messageAttributes.put("Type", new MessageAttributeValue().withDataType("String")
				.withStringValue("terminate"));
		final SendMessageRequest sendMessageRequest = new SendMessageRequest();
		sendMessageRequest.withMessageBody("terminate");
		sendMessageRequest.withQueueUrl(Config.MANAGER_TASK_QUEUE_URL);
		sendMessageRequest.withMessageAttributes(messageAttributes);
		sqs.sendMessage(sendMessageRequest);
	}

	public List<Message> getMessage(String queueURL) {
		try {
			ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
					queueURL).withVisibilityTimeout(60);
			List<Message> messages = sqs
					.receiveMessage(
							receiveMessageRequest.withMessageAttributeNames("All"))
					.getMessages();
			return messages;
		} catch (AmazonClientException ace) {
			Config.handleException(ace);
		}
		return null;
	}

	public boolean isValidMessage(Map<String, MessageAttributeValue> atts) {
		return atts.containsKey("Type")
				&& atts.get("Type").getStringValue().equals("done task")
				&& atts.get("localAppID").getStringValue().equals(Config.LOCAL_ID);
	}

	public void deleteMessage(String messageRecieptHandle, String queueURL) {
		try {
			System.out.println("deleting a message");
			sqs.deleteMessage(new DeleteMessageRequest(queueURL, messageRecieptHandle));
		} catch (AmazonClientException ace) {
			Config.handleException(ace);
		}
	}

	public void changeMessageVisibility(String managerAppDoneQueueUrl,
			String receiptHandle, Integer i) {
		sqs.changeMessageVisibility(managerAppDoneQueueUrl, receiptHandle, i);
	}

}
