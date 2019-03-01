package aws.dynamoDB.service;

import aws.dynamoDB.util.InvoiceStatus;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This is a service that works with the AWS Service - DynamoDB.
 * It includes methods to:
 * <ol>
 *     <li>CREATE an item</li>
 *     <li>GET an item</li>
 *     <li>UPDATE the status {@link aws.dynamoDB.util.InvoiceStatus} of an item</li>
 *     <li>DELETE an item</li>
 * </ol>
 */

public class DynamoService {

    private static Logger LOGGER = LoggerFactory.getLogger(DynamoService.class);
    private static DynamoDB dynamoDB;
    private static String tableName;

    private String region;

    public DynamoService(String region,String tableName) {
       this.tableName = tableName;
       this.region = region;
       dynamoDB = initDynamoDbClient();
    }

    public void createInvoiceItem(String fileName, String bucketName, String date, LocalDateTime time, String status) throws AWSException {
        checkNotNull(fileName, bucketName, date, time, status);

        String stringTime = formatTime(time);
        try {
            dynamoDB.getTable(tableName)
                    .putItem(new Item()
                            .withPrimaryKey("fileName", fileName, "date", date)
                            .withString("bucketName", bucketName)
                            .withString("moving_time", stringTime)
                            .withString("file_status", status));
            LOGGER.info("DynamoDB {} item created: fileName - {}, date - {}", tableName, fileName, date);
        } catch (Exception e) {
            LOGGER.error("Unable to add item {} - {} to DynamoDB table {}: {}", fileName, date, tableName, e.getMessage());
            throw new AWSException("Unable to add item: " + fileName + " " + date + " in " + tableName);
        }
    }

    public static Item getInvoiceItem(String fileName, String date) throws AWSException {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("fileName", fileName, "date", date);

        Table table;
        Item outcome;
        try {
            table = dynamoDB.getTable(tableName);

            LOGGER.debug("Attempting to read the item from DynamoDB table...");
            outcome = table.getItem(spec);
        }
        catch (Exception e) {
            LOGGER.error("Unable to read item: {} - {}: {}", fileName, date, e.getMessage());
            throw new AWSException("Unable to read item: " + fileName + " " + date);
        }
        if (outcome == null) {
            LOGGER.error("There is no such item {}-{} in table {}", fileName, date, tableName);
            throw new AWSException("There is no such item " + fileName + "-" + date + " in table " + tableName);
        }
        LOGGER.debug("GetItem from DynamoDB table succeeded: {}", outcome);
        return outcome;
    }

    public void updateInvoiceStatus(String fileName, String date, String status) throws AWSException{
       checkNotNull(fileName, date, status);

        Table table;
        UpdateItemSpec updateItemSpec;

        try {
            table = dynamoDB.getTable(tableName);

            updateItemSpec = new UpdateItemSpec().withReturnValues(ReturnValue.ALL_NEW)
                    .withPrimaryKey("fileName", fileName, "date", date)
                    .withUpdateExpression("set file_status = :file_status")
                    .withValueMap(new ValueMap()
                            .withString(":file_status", status));

            LOGGER.debug("Updating the item in DynamoDB table...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            LOGGER.info("Item [{} - {}] status in table \"{}\" updated -> {}", fileName, date, tableName, status);
        }
        catch (Exception e) {
            LOGGER.error("Unable to update item [{} - {}]: ", fileName, date, e.getMessage());
            throw new AWSException("Unable to update item DynamoDB table " +  fileName + " - " + e.getMessage());
        }
    }

    public void deleteInvoiceItem(String fileName, String date) throws AWSException {
        Table table;

        try {
            table = dynamoDB.getTable(tableName);

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("fileName", fileName, "date", date));

            LOGGER.debug("Deleting item from DynamoDB table...");
            table.deleteItem(deleteItemSpec);
            LOGGER.info("Item {} - {} successfully deleted", fileName, date);
        }
        catch (Exception e) {
            LOGGER.error("Unable to delete item {} - {} from DynamoDB table: {}", fileName, date, e.getMessage());
            throw new AWSException("Unable to delete item DynamoDB table: " + fileName + " " + date);
        }
    }

    public void updateInvoiceStatusAfterChecking(String firstKeyName, String secondKeyName, InvoiceStatus status) throws AWSException {
        List<Map<String, AttributeValue>> listOfItems = listInvoiceItemsToMove();
        try {
            if (!listOfItems.isEmpty()) {
                for (Map<String, AttributeValue> item : listOfItems) {
                    String fileName = String.valueOf(item.get(firstKeyName).getS());
                    String date = String.valueOf(item.get(secondKeyName).getS());
                    updateInvoiceStatus(fileName, date, String.valueOf(status));
                }
            }
        }catch (Exception e) {
            LOGGER.error("Error occurred while updating status in {} to {}: {}", tableName, status, e.getMessage());
            throw new AWSException("Error occurred while updating status in " + tableName + " to " + status + ": " + e.getMessage());
        }
    }

    public List<Map<String, AttributeValue>> listInvoiceItemsToMove() throws AWSException {
        try {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

            LocalDateTime now = LocalDateTime.now();
            String stringNow = formatTime(now);

            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":moving_time", new AttributeValue().withS(stringNow));
            expressionAttributeValues.put(":file_status", new AttributeValue().withS(InvoiceStatus.UPLOADED.toString()));

            ScanRequest scanRequest = new ScanRequest()
                    .withTableName(tableName)
                    .withFilterExpression("moving_time = :moving_time or moving_time < :moving_time and file_status = :file_status")
                    .withExpressionAttributeValues(expressionAttributeValues);

            ScanResult result = client.scan(scanRequest);
            return new ArrayList<>(result.getItems());
        }catch (Exception e) {
            LOGGER.error("Error occurred while getting list of items in table {}: {}", tableName ,e.getMessage());
            throw new AWSException("Error occurred while getting list of items in table " + tableName + ": " + e.getMessage());
        }
    }

    public static String formatTime(LocalDateTime time) {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        return dtf.format(time);
    }

    private DynamoDB initDynamoDbClient() {
        DynamoDB dynamoDB = null;
        try {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
            dynamoDB = new DynamoDB(client);
        } catch (Exception e) {
            LOGGER.error("Error while initializing DynamoDBClient: {}", e.getMessage());
        }
        return dynamoDB;
    }

    private void checkNotNull(String fileName, String bucketName, String date, LocalDateTime time, String status) throws AWSException {
        if (fileName == null || bucketName == null ||tableName == null || date == null || status == null || time == null) {
            LOGGER.error("Can't create DynamoDB record: table name-{}, file name-{}, bucket name-{}, date-{}, status-{}, time-{}",
                    tableName, fileName, bucketName, date, status, time);
            throw new AWSException("Can't create DynamoDB record: table name-" + tableName + ", file name-" + fileName +
                    "bucket name -" + bucketName + ", date-" + date + ", status-" + status + ", time-" + time);
        }
    }

    private void checkNotNull(String fileName, String date, String status) throws AWSException {
        if (fileName == null || tableName == null || date == null || status == null) {
            LOGGER.error("Can't create DynamoDB record: table name-{}, file name-{}, date-{}, status-{}", tableName, fileName, date, status);
            throw new AWSException("Can't create DynamoDB record: table name-" + tableName + ", file name-" + fileName + ", date-" + date + ", status-" + status);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

}
