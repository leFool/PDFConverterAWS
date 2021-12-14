package worker;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pdfbox.pdmodel.PDDocument;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import main.Config;

public class Worker {

	@SuppressWarnings("unused")
	private static int workerID;
	private static String action, originalUrl, localAppID, messageReceipt,
			convertedFileURL;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;

	public static void main(String[] args) throws Exception {
		workerID = (args.length == 0) ? 0 : Integer.parseInt(args[0]);
		s3 = AmazonS3ClientBuilder.defaultClient();
		sqs = AmazonSQSClientBuilder.defaultClient();
		work();
	}

	private static void work() {
		while (true) {
			try {
				messageReceipt = null;
				ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(
						Config.WORKER_TASK_QUEUE_URL).withVisibilityTimeout(600);
				List<Message> messages = sqs
						.receiveMessage(
								receiveMessageRequest.withMessageAttributeNames("All"))
						.getMessages();
				if (messages.isEmpty()) {
					System.out.println("sleeping, no new message");
					Thread.sleep(100);
					continue;
				}
				Message msg = messages.get(0);
				messageReceipt = msg.getReceiptHandle();
				Map<String, MessageAttributeValue> messageAttributes = msg
						.getMessageAttributes();
				if (!messageAttributes.containsKey("Type") || !messageAttributes
						.get("Type").getStringValue().equals("PDF task"))
					throw new Exception("task missing attributes: Operation " + action
							+ " URL: " + originalUrl + "\n");
				// if
				// (Integer.parseInt(messageAttributes.get("workerID")
				// .getStringValue()) != workerID)
				// continue;
				action = messageAttributes.get("Action").getStringValue();
				originalUrl = messageAttributes.get("URL").getStringValue();
				localAppID = messageAttributes.get("localAppID").getStringValue();
				System.out.println("Action: " + action);
				System.out.println("URL: " + originalUrl);
				convertedFileURL = processMsg(originalUrl, action);
				System.out.println("worker finished task: Operation " + action + " URL: "
						+ originalUrl + ", s3url: " + convertedFileURL);
				sendCompletionMessage(action, originalUrl, convertedFileURL);
				sqs.deleteMessage(new DeleteMessageRequest(Config.WORKER_TASK_QUEUE_URL,
						messageReceipt));
				System.out.println("done");
			} catch (Exception e) {
				e.printStackTrace();
				sendCompletionMessage(action, originalUrl, e.getMessage());
				if (messageReceipt != null) {
					System.out.println("delete Message from sqs queue");
					sqs.deleteMessage(new DeleteMessageRequest(
							Config.WORKER_TASK_QUEUE_URL, messageReceipt));
				}
			}
		}
	}

	private static String processMsg(String rawUrl, String op) throws Exception {
		String[] urlSplit = rawUrl.split("/");
		String fileUniqueName = urlSplit[urlSplit.length - 1];
		System.setProperty("http.agent", "Chrome");
		URL url = new URL(rawUrl);
		InputStream stream = url.openConnection().getInputStream();
		PDDocument resultFile = PDDocument.load(stream);
		if (fileUniqueName == null)
			System.out.println("null File " + rawUrl);
		File convertedFile;
		switch (op) {
			case "ToImage":
				convertedFile = Converter.convertPDFToImage(resultFile);
				break;
			case "ToHTML":
				convertedFile = Converter.convertPDFToHTML(resultFile);
				break;
			case "ToText":
				convertedFile = Converter.convertPDFToText(resultFile);
				break;
			default:
				throw new Exception("unknown task operation");
		}
		String key = (convertedFile.getName() + localAppID).replace('\\', '_')
				.replace('/', '_').replace(':', '_');
		PutObjectRequest req = new PutObjectRequest(Config.CONVERTED_FILES_BUCKET_NAME,
				key, convertedFile);
		s3.putObject(req.withCannedAcl(CannedAccessControlList.PublicRead));
		deleteFile(convertedFile);
		return s3.getUrl(Config.CONVERTED_FILES_BUCKET_NAME, key).toString();
	}

	private static void sendCompletionMessage(String op, String originalURL,
			String output) {
		Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
		messageAttributes.put("Type", new MessageAttributeValue().withDataType("String")
				.withStringValue("done PDF task"));
		messageAttributes.put("localAppID", new MessageAttributeValue()
				.withDataType("String").withStringValue(localAppID));
		messageAttributes.put("originalURL", new MessageAttributeValue()
				.withDataType("String").withStringValue(originalURL));
		messageAttributes.put("operation",
				new MessageAttributeValue().withDataType("String").withStringValue(op));
		messageAttributes.put("output", new MessageAttributeValue().withDataType("String")
				.withStringValue(output));
		SendMessageRequest sendMessageRequest = new SendMessageRequest(
				Config.WORKER_DONE_QUEUE_URL, "done PDF task");
		sendMessageRequest.withMessageAttributes(messageAttributes);
		sqs.sendMessage(sendMessageRequest);
	}

	static void deleteFile(File file) {
		if (file.delete()) {
			System.out.println("File " + file.getName() + " deleted successfully");
			return;
		}
		System.out.println("Failed to delete the file: " + file.getName());
	}

}
