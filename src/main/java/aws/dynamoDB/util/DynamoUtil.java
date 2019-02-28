package aws.dynamoDB.util;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static aws.dynamoDB.service.DynamoService.getInvoiceItem;

public class DynamoUtil {

    private static Logger LOGGER = LoggerFactory.getLogger(DynamoUtil.class);

    public static boolean isFileReadyToMove(DynamodbEvent dynamodbEvent, String fileName, String date, String attrName, String attrValue) throws AWSException {
        boolean isEventNameModify = dynamodbEvent.getRecords().get(0).getEventName().equals("MODIFY");
        Item item = getInvoiceItem(fileName, date);
        boolean isFileStatusMoving = item.getString(attrName).equals(attrValue);

        return isEventNameModify && isFileStatusMoving;
    }

    public static String getInvoiceTableKey(DynamodbEvent dynamodbEvent, String keyName) throws AWSException {
        LOGGER.debug("Getting value for key {}...", keyName);
        String value = "";
        try {
            if (dynamodbEvent.getRecords().get(0).getEventName().equals("MODIFY")) {
                for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {
                    value = record.getDynamodb().getKeys().get(keyName).getS();
                }
            }
        }catch (Exception e) {
            LOGGER.error("Error occurred while getting value for attribute {}: {}", keyName, e.getMessage());
            throw new AWSException("Error occurred while getting value for attribute " + keyName + ": " + e.getMessage());
        }
        LOGGER.info("{} - {}", keyName, value);
        return value;
    }

    public static String getBucketName(String fileName, String date) throws AWSException {
        return getAttributeValue(fileName, date, "bucketName");
    }

    public static String getAttributeValue(String fileName, String date, String attributeName) throws AWSException {
        try {
            LOGGER.debug("Getting value for attribute {}, with keys {}, {}", attributeName, fileName, date);
            Item item = getInvoiceItem(fileName, date);
            LOGGER.debug("Value - {}", item.getString(attributeName));
            return item.getString(attributeName);
        } catch (Exception e) {
            LOGGER.error("Error occurred while getting bucketName attribute value: {}", e.getMessage());
            throw new AWSException("Error occurred while getting bucketName attribute value: " + e.getMessage());
        }
    }

    public static String getKeyWithFolder(String folderName, String fileName) throws AWSException {
        return folderName + "/" + fileName;
    }
}
