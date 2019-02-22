package aws.sqs.util;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import exception.AWSException;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a util class that works with SQS.
 * It contains methods that get information about the uploaded file
 * from the SQS message body
 */

public class SQSUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(SQSUtil.class);

    public static String getDestinationKey(String destinationFolderName, SQSEvent sqsEvent) throws AWSException {
        return destinationFolderName + "/" + getFileName(sqsEvent);
    }

    public static String getSourceKey(String sourceFolderName, SQSEvent sqsEvent) throws AWSException {
        return sourceFolderName + "/" + getFileName(sqsEvent);
    }

    public static String getBucketName(SQSEvent sqsEvent) throws AWSException {
        try {
            return getMessageBody(sqsEvent).getString("bucketName");
        }catch (JSONException e) {
            LOGGER.error("Object [bucketName] not found in SQS message: " + e.getMessage());
            throw new AWSException("Object [bucketName] not found in SQS message");
        }
    }

    public static String getFileName(SQSEvent sqsEvent) throws AWSException {
        try {
            return getMessageBody(sqsEvent).getString("fileName");
        }catch (JSONException e) {
            LOGGER.error("Object [fileName] not found in SQS message: " + e.getMessage());
            throw new AWSException("Object [fileName] not found in SQS message");
        }
    }

    public static String getDate(SQSEvent sqsEvent) throws AWSException {
        try {
            return getMessageBody(sqsEvent).getString("date");
        }catch (JSONException e) {
            LOGGER.error("Object [date] not found in SQS message: " + e.getMessage());
            throw new AWSException("Object [date] not found in SQS message");
        }
    }

    private static JSONObject getMessageBody(SQSEvent sqsEvent)  throws AWSException {
        try {
            String body = SQSUtil.getMessageFromSQS(sqsEvent);
            return new JSONObject(body);
        } catch (JSONException e) {
            LOGGER.error("Error while reading SQS JSON message: " + e.getMessage());
            throw new AWSException("Error while reading SQS JSON message: " + e.getMessage());
        }
    }

    private static String getMessageFromSQS(SQSEvent sqsEvent) throws AWSException {
        String body = "";
        try {
            for (SQSEvent.SQSMessage msg : sqsEvent.getRecords()) {
                body = msg.getBody();
            }
        }catch (Exception e) {
            LOGGER.error("Error while getting message body from SQS: " + e);
            throw new AWSException("Error while getting message body from SQS: " + e);
        }
        return body;
    }

    public static String generateMessage(String bucketName, String fileName,  String date) throws AWSException {
       verifyDataNotNull(bucketName, fileName, date);

        ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

        SQSMessage msg = createObject(bucketName, fileName, date);
        String jsonInString = null;

        try {
            jsonInString = ow.writeValueAsString(msg);
            LOGGER.info("SQS message successfully created [{}]", jsonInString);
        } catch (JsonProcessingException e) {
            LOGGER.error("Error occurred while creating SQS message for file [{}] in bucket [{}] in JSON format", fileName, bucketName);
        }

        return jsonInString;
    }

    private static SQSMessage createObject(String bucketName, String fileName, String date) {
        SQSMessage sqsMessage = new SQSMessage();

        sqsMessage.setFileName(fileName);
        sqsMessage.setBucketName(bucketName);
        sqsMessage.setDate(date);

        return sqsMessage;
    }

    private static void verifyDataNotNull(String bucketName, String fileName,  String date) throws AWSException {
        if (fileName == null || bucketName == null || date == null) {
            LOGGER.error("fileName[{}], bucketName[{}], date[{}]", fileName, bucketName, date);
            throw new AWSException("Can't generate SQS message: file name[" + fileName + "], bucket name[" + bucketName + "], date[" + date + "]");
        }
    }

}
