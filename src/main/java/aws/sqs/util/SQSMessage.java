package aws.sqs.util;

/**
 * This class represents an SQS message.
 * It is used to generate an SQS message in JSON format.
 */

public class SQSMessage {
    private String fileName;
    private String bucketName;
    private String date;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getFileName() {
        return fileName;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
}