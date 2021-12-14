package manager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import main.Config;

public class LocalAppHandler implements Runnable {

	private int numOfTasks, neededWorkers, tasksPerWorker;
	private String localID;
	private Set<String> urls;
	private Message msg;

	public LocalAppHandler(Message msg) {
		urls = new HashSet<String>();
		this.msg = msg;
	}

	@Override
	public void run() {
		Map<String, MessageAttributeValue> msgAttributes = msg.getMessageAttributes();
		if (Manager.isTerminationMessage(msgAttributes)) {
			Manager.isRunning = false;
			Manager.clearAppManagerSQS(Manager.sqs, msg);
			return;
		}
		String inputFileName = msgAttributes.get("FileName").getStringValue();
		localID = msgAttributes.get("localAppID").getStringValue();
		tasksPerWorker = Integer
				.parseInt(msgAttributes.get("tasksPerWorker").getStringValue());
		configureParameters(inputFileName);
		Manager.openWorkers(neededWorkers);
		parseFileIntoMessagesAndInformWorkers();
		Manager.clearAppManagerSQS(Manager.sqs, msg);
	}

	private void configureParameters(String inputFileName) {
		downloadTasks(inputFileName);
		setNumOfTasks();
		neededWorkers = (numOfTasks % tasksPerWorker == 0) ? numOfTasks / tasksPerWorker
				: numOfTasks / tasksPerWorker + 1;
	}

	private void downloadTasks(String inputFileName) {
		System.out.println("Downloading task file");
		S3Object taskFile = Manager.s3.getObject(
				new GetObjectRequest(Config.PENDING_FILES_BUCKET_NAME, inputFileName));
		InputStream is = taskFile.getObjectContent();
		BufferedReader in = null;
		urls.clear();
		try {
			in = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = in.readLine()) != null) {
				urls.add(line);
			}
		} catch (Exception e) {} finally {
			if (in != null)
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	private void setNumOfTasks() {
		numOfTasks = urls.size();
	}

	private void parseFileIntoMessagesAndInformWorkers() {
		List<Task> tasks = new ArrayList<Task>();
		int i = 0;
		for (String rawTask : urls) {
			String[] taskSplitted = rawTask.split("\t");
			SendMessageRequest msgRequest = new SendMessageRequest(
					Config.WORKER_TASK_QUEUE_URL, "PDF task");
			Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
			messageAttributes.put("Type", new MessageAttributeValue()
					.withDataType("String").withStringValue("PDF task"));
			messageAttributes.put("Action", new MessageAttributeValue()
					.withDataType("String").withStringValue(taskSplitted[0]));
			messageAttributes.put("URL", new MessageAttributeValue()
					.withDataType("String").withStringValue(taskSplitted[1]));
			messageAttributes.put("localAppID", new MessageAttributeValue()
					.withDataType("String").withStringValue(localID));
			messageAttributes.put("workerID",
					new MessageAttributeValue().withDataType("String")
							.withStringValue(String.valueOf(i++ % neededWorkers))); // round
																					// robin
																					// (ignored
																					// by
																					// workers
																					// to
																					// ensure
																					// work
																					// is
																					// done)
			msgRequest.withMessageAttributes(messageAttributes);
			Manager.sqs.sendMessage(msgRequest);
			tasks.add(new Task(taskSplitted[0], taskSplitted[1]));
		}
		Manager.tasksOfLocalApp.put(localID, tasks);
	}

}
