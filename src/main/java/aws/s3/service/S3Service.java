package aws.s3.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a service that works with the AWS Service - S3 Bucket.
 * It has a method {@link aws.s3.service.S3Service#moveFile(String, String, String)} that moves (copies and deletes) a file from one S3 bucket directory to another.
 */

public class S3Service {

    private Logger LOGGER = LoggerFactory.getLogger(S3Service.class);
    private static String REGION = System.getenv("REGION");

    private AmazonS3 s3Client = initS3Client();

    public void moveFile(String bucketName, String sourceKey, String destinationKey) throws AWSException {
       verifyDataNotNull(bucketName, sourceKey, destinationKey);

        try {
            LOGGER.debug("Copying file to S3Bucket...");
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, sourceKey, bucketName, destinationKey);
            s3Client.copyObject(copyObjRequest);
            LOGGER.info("File [{}] successfully copied to [moved] directory", sourceKey, bucketName);
        }catch (Exception e) {
            LOGGER.error("Can't copy file " + sourceKey + ": " + e.getMessage());
            throw new AWSException("Can't copy file " + sourceKey + ": " + e.getMessage());
        }

        try {
            LOGGER.debug("Deleting file from S3Bucket [{}]...", bucketName);
            s3Client.deleteObject(new DeleteObjectRequest(bucketName, sourceKey));
            LOGGER.info("File [{}] successfully deleted", sourceKey);
            LOGGER.info("File successfully moved from [{}] to [{}]", sourceKey.split("/")[0], destinationKey.split("/")[0]);
        }catch (Exception e) {
            LOGGER.error("Can't delete file " + sourceKey + ": " + e.getMessage());
            throw new AWSException("Can't delete file " + sourceKey + ": " + e.getMessage());
        }
    }

    private AmazonS3 initS3Client() {
        AmazonS3 amazonS3 = null;
        try {
            amazonS3 = AmazonS3ClientBuilder.standard().withRegion(REGION).build();;
        }catch (Exception e) {
            LOGGER.error("Error occurred while initializing S3 Client: " + e.getMessage());
        }
        return amazonS3;
    }

    private void verifyDataNotNull(String bucketName, String sourceKey, String destinationKey) throws AWSException {
        if (bucketName == null || sourceKey == null || destinationKey == null) {
            LOGGER.error("Can't move file to S3 Bucket: bucket[{}], sourceKey[{}], destinationKey[{}]", bucketName, sourceKey, destinationKey);
            throw new AWSException("Can't move file to S3 Bucket: bucket[" + bucketName + "], sourceKey[" + sourceKey + "], destinationKey[" + destinationKey + "]");
        }
    }

}
