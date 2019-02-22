package aws.sqs.service;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This is a service that works with the AWS Service - SQS.
 * It has a method that sends a message to SQS that contains:
 * <ol>
 *     <li>Name of the uploaded file</li>
 *     <li>Bucket name in which the file was uploaded</li>
 *     <li>Date with time of file uploading</li>
 * </ol>
 */

public class SQSService {

    private Logger LOGGER = LoggerFactory.getLogger(SQSService.class);

    private String region;
    private String sourceQueueUrl;

    private AmazonSQS sqs;

    public SQSService(String region, String sourceQueueUrl, String deadLetterQueueName) {
        this.region = region;
        this.sourceQueueUrl = sourceQueueUrl;
        sqs = initSQSClient();
        setDeadLetterQueue(sourceQueueUrl, deadLetterQueueName);
    }

    public void sendMessage(String message) throws AWSException {
        if (message == null || message.isEmpty()) {
            throw new AWSException("Can't send SQS message, because SQS message body is null or empty");
        }
        if (sqs == null) {
            throw new AWSException("Can't create SQS message, because SQS client is null");
        }

        LOGGER.debug("Sending msg to SQS - {}", message);

        try {
            sqs.sendMessage(new SendMessageRequest()
                    .withMessageBody(message)
                    .withQueueUrl(sourceQueueUrl));
        }catch (Exception e) {
            LOGGER.error("Error while creating SQS message: ", e.getMessage());
            throw new AWSException("Error while creating SQS message: " + e.getMessage());
        }

        LOGGER.info("SQS message successfully sent");
    }

    public void deleteAllMessages() throws AWSException {
        LOGGER.debug("Receiving messages from Queue...");
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sourceQueueUrl).withMaxNumberOfMessages(10);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            LOGGER.info("Messages in SQS -{}", messages.size());

            for (Message m : messages) {
                LOGGER.debug("Deleting message {}", m);
                sqs.deleteMessage(sourceQueueUrl, m.getReceiptHandle());
                LOGGER.info("Message {} successfully deleted", m);
            }
        }catch (Exception e) {
            LOGGER.error("Unable to delete messages from SQS {}: {}", sourceQueueUrl, e.getMessage());
            throw new AWSException("Unable to delete messages from SQS " + sourceQueueUrl + ": " + e.getMessage());
        }
    }

    public void setDeadLetterQueue(String sourceQueueUrl, String deadLetterQueueName) {
        try {
            String deadLetterQueueArn = getDeadLetterQueueArn(deadLetterQueueName);

            SetQueueAttributesRequest request = new SetQueueAttributesRequest()
                    .withQueueUrl(sourceQueueUrl)
                    .addAttributesEntry(QueueAttributeName.RedrivePolicy.toString(),
                            "{\"maxReceiveCount\":\"3\", \"deadLetterTargetArn\":\""
                                    + deadLetterQueueArn + "\"}");
            sqs.setQueueAttributes(request);

            LOGGER.info("Set queue {} as source queue for dead-letter queue {}", sourceQueueUrl, deadLetterQueueName);
        } catch (AmazonServiceException ase) {
            LOGGER.error("Error occurred while setting deadLetterQueue. Caught an AmazonServiceException, Error Message:    " + ase.getMessage());
        } catch (AmazonClientException ace) {
            LOGGER.error("Error occurred while setting deadLetterQueue. Caught an AmazonClientException, Error Message: " + ace.getMessage());
        } catch (AWSException e) {
            LOGGER.error("Error occurred while setting deadLetterQueue: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private String getDeadLetterQueueArn(String deadLetterQueueName) throws AWSException {
        try {
            String deadLetterQueueUrl = sqs.getQueueUrl(deadLetterQueueName)
                    .getQueueUrl();
            GetQueueAttributesResult deadLetterQueueAttributes = sqs.getQueueAttributes(
                    new GetQueueAttributesRequest(deadLetterQueueUrl)
                            .withAttributeNames("QueueArn"));
            return deadLetterQueueAttributes.getAttributes().get("QueueArn");
        } catch (AmazonServiceException ase) {
            LOGGER.error("Caught an AmazonServiceException, Error Message: {}", ase.getMessage());
            throw new AWSException("Caught an AmazonServiceException, Error Message: " + ase.getMessage());
        } catch (AmazonClientException ace) {
            LOGGER.error("Caught an AmazonClientException, Error Message: {}", ace.getMessage());
            throw new AWSException("Caught an AmazonClientException, Error Message: " + ace.getMessage());
        }
    }

    private AmazonSQS initSQSClient() {
        AmazonSQS client = null;
        try {
            client = AmazonSQSClientBuilder.standard().withRegion(region).build();
        }catch (Exception e) {
            LOGGER.error("Error while initializing SQSClient: ", e.getMessage());
        }
        return client;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getSourceQueueUrl() {
        return sourceQueueUrl;
    }

    public void setSourceQueueUrl(String sourceQueueUrl) {
        this.sourceQueueUrl = sourceQueueUrl;
    }

}
