package local;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import main.Config;

public class Storage {

	private final AmazonS3 s3;
	private final MessageQueue msgq;

	public Storage(MessageQueue msgq) {
		this.msgq = msgq;
		s3 = AmazonS3ClientBuilder.defaultClient();
		if (!isBucketExist(Config.CONVERTED_FILES_BUCKET_NAME)) {
			createBucket(Config.CONVERTED_FILES_BUCKET_NAME);
		}
		if (!isBucketExist(Config.JAR_BUCKET_NAME)) {
			createBucket(Config.JAR_BUCKET_NAME);
			uploadJarToBucket();
		}
		if (!isBucketExist(Config.PENDING_FILES_BUCKET_NAME))
			createBucket(Config.PENDING_FILES_BUCKET_NAME);
	}

	private boolean isBucketExist(String bucketName) {
		for (Bucket bucket : s3.listBuckets()) {
			if (bucket.getName().equals(bucketName)) {
				System.out.println(
						"bucket found, no need to create new bucket: " + bucketName);
				return true;
			}
		}
		System.out.println("bucket not found, creating new bucket: " + bucketName);
		return false;
	}

	public void createBucket(String bucketName) {
		try {
			s3.createBucket(bucketName);
		} catch (AmazonClientException e) {
			Config.handleException(e);
		}
	}

	public void uploadJarToBucket() {
		try {
			File f = new File(Config.JAR_NAME);
			PutObjectRequest req = new PutObjectRequest(Config.JAR_BUCKET_NAME,
					Config.JAR_NAME, f).withCannedAcl(CannedAccessControlList.PublicRead);
			s3.putObject(req);
		} catch (AmazonClientException e) {
			Config.handleException(e);
		}
	}

	public void uploadToBucketAndNotifyManager() {
		try {
			File f = new File(Config.inputFileName);
			String key = (f.getName() + Config.LOCAL_ID).replace('\\', '_')
					.replace('/', '_').replace(':', '_');
			PutObjectRequest req = new PutObjectRequest(Config.PENDING_FILES_BUCKET_NAME,
					key, f).withCannedAcl(CannedAccessControlList.PublicRead);
			s3.putObject(req);
			msgq.sendTaskMessageToManager(key);
		} catch (AmazonClientException e) {
			Config.handleException(e);
		}
	}

	public void getObjectFromBucket(String key) {
		try {
			System.out.println("Downloading an object");
			s3.getObject(new GetObjectRequest(Config.CONVERTED_FILES_BUCKET_NAME, key),
					new File(key));
		} catch (AmazonClientException e) {
			Config.handleException(e);
		}
	}

	public void deleteFileFromBucket(String bucketName, String key) {
		try {
			System.out.println("Deleting an object");
			s3.deleteObject(bucketName, key);
			System.out.println();
		} catch (AmazonClientException e) {
			Config.handleException(e);
		}
	}

}
