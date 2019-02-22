package aws.dynamoDB.util;

/**
 * This is an enum of file statuses available to set in DynamoDB table item:
 * <ol>
 *     <li>COPIED - SQS message and DynamoDB item created, according to the file uploaded to S3 bucket directory</li>
 *     <li>UPLOADED - file moved from one S3 Bucket directory to another and item status in DynamoDB table edited</li>
 * </ol>
 */

public enum InvoiceStatus {
    COPIED,
    UPLOADED
}
