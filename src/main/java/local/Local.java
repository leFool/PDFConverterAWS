package local;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import main.Config;

public class Local {

	public static void main(String[] args) {
		System.out.println("starting local app with id: " + Config.LOCAL_ID);
		if (args.length > 0)
			parseArgs(args);
		Cloud ec2 = new Cloud();
		MessageQueue msgq = new MessageQueue();
		Storage s3 = new Storage(msgq);
		try {
			s3.uploadToBucketAndNotifyManager();
			if (!ec2.isExist()) 
				ec2.run();
			else
				System.out.println("manager already exist!");
			while (!messageLoop(ec2, msgq, s3))
				;
			if (Config.shouldTerminate) {
				msgq.sendTerminateMsgToManager();
			}
			System.out.println("local app done");
		} catch (Exception e) {
			Config.handleException(e);
		}
	}

	private static void parseArgs(String[] args) {
		Config.inputFileName = args[0];
		Config.outputFileName = args[1];
		Config.tasksPerWorker = Integer.parseInt(args[2]);
		Config.shouldTerminate = (args.length > 3);
	}

	private static boolean messageLoop(Cloud ec2, MessageQueue sqs, Storage s3)
			throws IOException {
		while (true) {
			List<Message> messages = sqs.getMessage(Config.MANAGER_DONE_QUEUE_URL);
			if (!messages.isEmpty()) {
				return parseMessage(sqs, s3, messages.get(0));
			}
			try {
				Thread.sleep(100);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static boolean parseMessage(MessageQueue msgq, Storage s3, Message msg)
			throws IOException {
		Map<String, MessageAttributeValue> messageAttributes = msg.getMessageAttributes();
		if (!msgq.isValidMessage(messageAttributes))
			return false;
		msgq.changeMessageVisibility(Config.MANAGER_DONE_QUEUE_URL,
				msg.getReceiptHandle(), 600);
		String sumFileURL = messageAttributes.get("URL").getStringValue();
		System.out.println("Local app got done task message");
		System.out.println("FileBucketUrl: " + sumFileURL);
		System.out.println("downloading file");
		s3.getObjectFromBucket(sumFileURL);
		s3.deleteFileFromBucket(Config.CONVERTED_FILES_BUCKET_NAME, sumFileURL);
		createSummaryHTMLFile(sumFileURL);
		msgq.deleteMessage(msg.getReceiptHandle(), Config.MANAGER_DONE_QUEUE_URL);
		return true;
	}

	private static void createSummaryHTMLFile(String summaryFileName) {
		try {
			File sumFile = new File(summaryFileName);
			File outputFile = new File(Config.outputFileName);
			InputStream is = new FileInputStream(sumFile);
			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			String line;
			PrintWriter out = new PrintWriter(outputFile, "UTF-8");
			out.println(
					"<html><h2>Distributed System Programming: Assignment 1</h2><h3>Results of LocalApp ID: "
							+ Config.LOCAL_ID + "</h3><br />");
			out.println("<body>");
			while ((line = in.readLine()) != null)
				out.println(line + "<br />");
			in.close();
			out.println("</body></html>");
			out.close();
			sumFile.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
