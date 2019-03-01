package aws.s3.util;

import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.event.S3EventNotification;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

/**
 * This is a util class that works with S3 Bucket.
 * It contains methods to get the name {@link aws.s3.util.S3Util#getFileName(S3Event)}
 * and bucket name {@link aws.s3.util.S3Util#getS3BucketName(S3Event)} of the uploaded file to S3 bucket
 */

public class S3Util {

    private static Logger LOGGER = LoggerFactory.getLogger(S3Util.class);

    private static Map<String, String> mapOfFilePaths = new HashMap<>();

    public static String getS3BucketName(S3Event s3Event) throws AWSException {
        S3EventNotification.S3EventNotificationRecord record = getS3Record(s3Event);

        checkNotNull(record);

        String bkt;
        try {
            bkt = record.getS3().getBucket().getName();
            LOGGER.debug("Bucket name successfully received {}", bkt);
        }catch (Exception e) {
            LOGGER.error("Error while getting S3 bucket name: {}", e.getMessage());
            throw new AWSException("Error while getting S3 bucket name: " + e.getMessage());
        }
        return bkt;
    }

    public static String getFileName(S3Event s3Event) throws AWSException {
        try {
            String fileName = getFileKey(s3Event).split("/")[1];
            checkFileExtensionIsXml(fileName);
            return fileName;
        }catch (Exception e) {
            LOGGER.error("Error while getting file name: {}", e.getMessage());
            throw new AWSException("Error while getting file name: " + e.getMessage());
        }
    }

    public static String getFileKey(S3Event s3Event) throws AWSException {
        S3EventNotification.S3EventNotificationRecord record = getS3Record(s3Event);

        checkNotNull(record);

        String key;
        try {
            key = record.getS3().getObject().getKey().replace('+', ' ');
            key = URLDecoder.decode(key, "UTF-8");
        } catch (Exception e) {
            LOGGER.error("Error while getting file key: {}", e.getMessage());
            throw new AWSException("Error while getting file key: " + e.getMessage());
        }
        LOGGER.debug("File key successfully received - {}", key);
        return key;
    }

    private static S3EventNotification.S3EventNotificationRecord getS3Record(S3Event s3Event) throws AWSException {
        if (s3Event == null) {
            LOGGER.error("Error while getting S3 Event Notification record - S3 Event is null");
            throw new AWSException("Error while getting S3 Event Notification record - S3 Event is null");
        }

        S3EventNotification.S3EventNotificationRecord record = null;
        try {
            if (!s3Event.getRecords().isEmpty()) {
                record = s3Event.getRecords().get(0);
            }
        } catch (Exception e) {
            LOGGER.error("Error while getting S3 Event Notification record");
            throw new AWSException("Error while getting S3 Event Notification record");
        }
        return record;
    }

    public static void checkFileExtensionIsXml(String fileName) throws AWSException {
        String extension = "";
        if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0) {
            extension = fileName.substring(fileName.lastIndexOf(".") + 1);
        }
        if (!extension.equals("xml")) {
            LOGGER.error("Error: file extention is not xml - {}", extension);
            throw new AWSException("Error: file extention is not xml - " + extension);
        }

    }

    private static void checkNotNull(S3EventNotification.S3EventNotificationRecord record) throws AWSException {
        if (record == null) {
            LOGGER.error("Error while getting file name - S3EventNotificationRecord is null");
            throw new AWSException("Error while getting file name - S3EventNotificationRecord is null");
        }
    }
}
