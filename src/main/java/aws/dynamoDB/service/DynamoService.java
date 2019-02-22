package aws.dynamoDB.service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Logger LOGGER = LoggerFactory.getLogger(DynamoService.class);

    private String tableName;
    private String region ;
    private DynamoDB dynamoDB ;

    public DynamoService(String region,String tableName) {
       this.tableName = tableName;
       this.region = region;

        dynamoDB = initDynamoDbClient();
    }

    public void createInvoiceItem(String fileName, String date, String status) throws AWSException {
        checkNotNull(fileName, date, status);

        if (fileName == "nullPointer")
            throw new NullPointerException("For test");

        try {
            dynamoDB.getTable(tableName)
                    .putItem(new Item()
                            .withPrimaryKey("fileName", fileName, "date", date)
                            .withString("file_status", status));
            LOGGER.info("DynamoDB {} item created: fileName - {}, date - {}", tableName, fileName, date);
        } catch (Exception e) {
            LOGGER.error("Unable to add item {} - {} to DynamoDB table {}: {}", fileName, date, tableName, e.getMessage());
            throw new AWSException("Unable to add item: " + fileName + " " + date + " in " + tableName);
        }
    }

    public Item getInvoiceItem(String fileName, String date) throws AWSException {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("fileName", fileName, "date", date);

        Table table;
        try {
            table = dynamoDB.getTable(tableName);

            LOGGER.debug("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            LOGGER.info("GetItem succeeded: {}", outcome);
            return outcome;
        }
        catch (Exception e) {
            LOGGER.error("Unable to read item: {} - {}: {}", fileName, date, e.getMessage());
            throw new AWSException("Unable to read item: " + fileName + " " + date);
        }
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

            LOGGER.debug("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            LOGGER.info("Item [{} - {}] status in table \"{}\" updated: {} -> {}", fileName, date, tableName, "UPLOADED", status);
        }
        catch (Exception e) {
            LOGGER.error("Unable to update item [{} - {}]: ", fileName, date, e.getMessage());
            throw new AWSException("Unable to update item " +  fileName + " - " + e.getMessage());
        }
    }

    public void deleteInvoiceItem(String fileName, String date) throws AWSException {
        Table table;

        try {
            table = dynamoDB.getTable(tableName);

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("fileName", fileName, "date", date));

            LOGGER.debug("Deleting item...");
            table.deleteItem(deleteItemSpec);
            LOGGER.info("Item {} - {} successfully deleted", fileName, date);
        }
        catch (Exception e) {
            LOGGER.error("Unable to delete item {} - {}: {}", fileName, date, e.getMessage());
            throw new AWSException("Unable to delete item: " + fileName + " " + date);
        }
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
