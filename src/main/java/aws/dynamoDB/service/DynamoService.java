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
 *     <li>UPDATE the status {@link aws.dynamoDB.util.INVOICE_STATUS} of an item</li>
 *     <li>DELETE an item</li>
 * </ol>
 */

public class DynamoService {

    private Logger LOGGER = LoggerFactory.getLogger(DynamoService.class);

    private static String TABLE_NAME;
    private static String REGION = System.getenv("REGION");

    public DynamoService(String TABLE_NAME) {
        DynamoService.TABLE_NAME = TABLE_NAME;
    }

    private DynamoDB dynamoDB = initDynamoDbClient();

    public void createInvoiceItem(String fileName, String date, String status) throws AWSException {
        verifyDynamoDBDataNotNull(fileName, date, status);

        try {
            dynamoDB.getTable(TABLE_NAME)
                    .putItem(new Item()
                            .withPrimaryKey("fileName", fileName, "date", date)
                            .withString("file_status", status));
            LOGGER.info("PutItem succeeded: fileName [{}], date [{}]", fileName, date);
        } catch (Exception e) {
            LOGGER.error("Unable to add item: " + fileName + " " + date + " in " + TABLE_NAME);
            LOGGER.error(e.getMessage());
            throw new AWSException("Unable to add item: " + fileName + " " + date + " in " + TABLE_NAME);
        }
    }

    public Item getItem(String fileName, String date) throws AWSException {
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("fileName", fileName, "date", date);

        Table table;
        try {
            table = dynamoDB.getTable(TABLE_NAME);

            LOGGER.debug("Attempting to read the item...");
            Item outcome = table.getItem(spec);
            LOGGER.info("GetItem succeeded: " + outcome);
            return outcome;
        }
        catch (Exception e) {
            LOGGER.error("Unable to read item: " + fileName + " " + date);
            LOGGER.error(e.getMessage());
            throw new AWSException("Unable to read item: " + fileName + " " + date);
        }
    }

    public void updateItemStatus(String fileName, String date, String status) throws AWSException{
       verifyDynamoDBDataNotNull(fileName, date, status);

        Table table;
        UpdateItemSpec updateItemSpec;

        try {
            table = dynamoDB.getTable(TABLE_NAME);

            updateItemSpec = new UpdateItemSpec().withReturnValues(ReturnValue.ALL_NEW)
                    .withPrimaryKey("fileName", fileName, "date", date)
                    .withUpdateExpression("set file_status = :file_status")
                    .withValueMap(new ValueMap()
                            .withString(":file_status", status));

            LOGGER.debug("Updating the item...");
            UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
            LOGGER.info("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
        }
        catch (Exception e) {
            LOGGER.error("Unable to update item [{}]", fileName);
            LOGGER.error(e.getMessage());
            throw new AWSException("Unable to update item [" +  fileName + "] - " + e.getMessage());
        }
    }

    public void deleteItem(String fileName, String date) throws AWSException {
        Table table;

        try {
            table = dynamoDB.getTable(TABLE_NAME);

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                    .withPrimaryKey(new PrimaryKey("fileName", fileName, "date", date));

            LOGGER.debug("Deleting item...");
            table.deleteItem(deleteItemSpec);
            LOGGER.info("DeleteItem succeeded");
        }
        catch (Exception e) {
            LOGGER.error("Unable to delete item: " + fileName + " " + date);
            LOGGER.error(e.getMessage());
            throw new AWSException("Unable to delete item: " + fileName + " " + date);
        }
    }

    private DynamoDB initDynamoDbClient() {
        DynamoDB dynamoDB = null;
        try {
            AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(REGION).build();
            dynamoDB = new DynamoDB(client);
        } catch (Exception e) {
            LOGGER.error("Error while initializing DynamoDBClient: " + e.getMessage());
        }
        return dynamoDB;
    }

    private void verifyDynamoDBDataNotNull(String fileName, String date, String status) throws AWSException {
        if (fileName == null || TABLE_NAME == null || date == null || status == null) {
            LOGGER.error("Can't create DynamoDB record: table name[{}], file name[{}], date[{}], status[{}]", TABLE_NAME, fileName, date, status);
            throw new AWSException("Can't create DynamoDB record: table name[" + TABLE_NAME + "], file name[" + fileName + "], date[" + date + "], status[" + status + "]");
        }
    }

    public String getTABLE_NAME() {
        return TABLE_NAME;
    }

    public void setTABLE_NAME(String TABLE_NAME) {
        this.TABLE_NAME = TABLE_NAME;
    }

    public String getREGION() {
        return REGION;
    }

    public void setREGION(String REGION) {
        this.REGION = REGION;
    }
}
