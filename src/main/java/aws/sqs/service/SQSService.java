package aws.sqs.service;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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

    private String REGION = System.getenv("REGION");

    private AmazonSQS sqs = initSQSClient(REGION);

    public void sendMessage(String sqs_url, String message) throws AWSException {
        if (message == null || message.isEmpty()) {
            throw new AWSException("Can't send SQS message, because SQS message body is null or empty");
        }
        if (sqs == null) {
            throw new AWSException("Can't create SQS message, because SQS client is null");
        }

        LOGGER.debug("Sending msg to SQS [{}]", message);
        SendMessageResult smr;

        try {
            smr = sqs.sendMessage(new SendMessageRequest()
                    .withMessageBody(message)
                    .withQueueUrl(sqs_url));
        }catch (Exception e) {
            LOGGER.error("Error while creating SQS message: " + e);
            throw new AWSException("Error while creating SQS message: " + e);
        }

        LOGGER.info("Creation of SQS message succeeded with messageId [{}]", smr.getMessageId());
    }

    public void deleteReceivedMessages(String sqs_url) throws AWSException {
        LOGGER.debug("Receiving messages from Queue...");
        try {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqs_url).withMaxNumberOfMessages(10);
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

            LOGGER.info("Messages in SQS [{}]", messages.size());

            for (Message m : messages) {
                LOGGER.debug("Deleting message [{}]", m);
                sqs.deleteMessage(sqs_url, m.getReceiptHandle());
                LOGGER.info("Message [{}] successfully deleted", m);
            }
        }catch (Exception e) {
            LOGGER.error("Unable to delete messages from SQS [{}] - {}", sqs_url, e.getMessage());
            throw new AWSException("Unable to delete messages from SQS [" + sqs_url + "] - " + e.getMessage());
        }
    }

    private AmazonSQS initSQSClient(String region) {
        AmazonSQS client = null;
        try {
            client = AmazonSQSClientBuilder.standard()
                    .withRegion(region)
                    .build();
        }catch (Exception e) {
            LOGGER.error("Error while initializing SQSClient: " + e);
        }
        return client;
    }
}
