package aws.s3.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import exception.AWSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This is a service that works with the AWS Service - S3 Bucket.
 * It has a method {@link aws.s3.service.S3Service#moveFile(String, String, String)} that moves (copies and deletes) a file from one S3 bucket directory to another.
 */

public class S3Service {

    private static Logger LOGGER = LoggerFactory.getLogger(S3Service.class);

    private AmazonS3 s3Client;
    private String region;

    public S3Service(String region) {
        this.region = region;
        s3Client = initS3Client();
    }

    public void moveFile(String bucketName, String sourceKey, String destinationKey) throws AWSException {
       checkNotNull(bucketName, sourceKey, destinationKey);

        try {
            LOGGER.debug("Copying file to S3 bucket...");
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, sourceKey, bucketName, destinationKey);
            s3Client.copyObject(copyObjRequest);
            LOGGER.debug("File {} successfully copied to \"moved\" directory", sourceKey);
        }catch (Exception e) {
            LOGGER.error("Can't copy file {}: {}", sourceKey, e.getMessage());
            throw new AWSException("Can't copy file " + sourceKey + ": " + e.getMessage());
        }

        try {
            LOGGER.debug("Deleting file from S3 bucket {}...", bucketName);
            s3Client.deleteObject(new DeleteObjectRequest(bucketName, sourceKey));
            LOGGER.debug("File {} successfully deleted from {}", sourceKey, sourceKey.split("/")[0]);
            LOGGER.info("File {} successfully moved from \"{}\" to \"{}\"", sourceKey, sourceKey.split("/")[0], destinationKey.split("/")[0]);
        }catch (Exception e) {
            LOGGER.error("Can't delete file {}: {}", sourceKey, e.getMessage());
            throw new AWSException("Can't delete file " + sourceKey + ": " + e.getMessage());
        }
    }

    public String getDate(String bucketName, String key) throws AWSException {
        Document doc = getXmlContent(bucketName, key);
        try {
            return doc.getElementsByTagName("date").item(0).getTextContent();
        }catch (Exception e) {
            LOGGER.error("There is no date tag in XML message {}: {}", key, e.getMessage());
            throw new AWSException("There is no date tag in XML message " + key + ": " + e.getMessage());
        }
    }

    public LocalDateTime getTime(String bucketName, String key) throws AWSException {
        Document doc = getXmlContent(bucketName, key);
        try {
            String time = doc.getElementsByTagName("time").item(0).getTextContent();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
            return LocalDateTime.parse(time, formatter);

        }catch (Exception e) {
            LOGGER.error("There is no time tag in XML message {}: {}", key, e.getMessage());
            throw new AWSException("There is no time tag in XML message " + key + ": " + e.getMessage());
        }
    }

    public Document getXmlContent(String bucketName, String key) throws AWSException {
        try {
            String message = getObjectContent(bucketName, key);
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource src = new InputSource();
            src.setEncoding("UTF-8");
            src.setCharacterStream(new StringReader(message));
            return builder.parse(src);
        } catch (Exception e) {
            LOGGER.error("Message {} is not valid XML: {}", key, e.getMessage());
            throw new AWSException("Message " + key + " is not valid XML: " + e.getMessage());
        }
    }

    public String getObjectContent(String bucketName, String key) throws AWSException {
        S3Object objectContent;
        String stringContent = "";
        LOGGER.debug("Downloading object {} from S3 bucket {}", key, bucketName);
        try {
            GetObjectRequest request = new GetObjectRequest(bucketName, key);
            objectContent = s3Client.getObject(request);
            stringContent = displayTextInputStream(objectContent.getObjectContent());
            LOGGER.debug("Content-Type: " + objectContent.getObjectMetadata().getContentType());
            LOGGER.info("File content: {}", stringContent);
            return stringContent;
        }catch (Exception e) {
            LOGGER.error("Error occurred while reading {} content: {}", key, e.getMessage());
            throw new AWSException("Error occurred while reading " + key + " content: " + e.getMessage());
        }
    }

    private static String displayTextInputStream(InputStream input) throws IOException{
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        StringBuilder content = new StringBuilder();
        String line = "";
        while ((line = reader.readLine()) != null) {
            content.append(line);
        }
        return content.toString();
    }

    private AmazonS3 initS3Client() {
        AmazonS3 amazonS3 = null;
        try {
            amazonS3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
        }catch (Exception e) {
            LOGGER.error("Error occurred while initializing S3 Client: " + e.getMessage());
        }
        return amazonS3;
    }

    private void checkNotNull(String bucketName, String sourceKey, String destinationKey) throws AWSException {
        if (bucketName == null || sourceKey == null || destinationKey == null) {
            LOGGER.error("Can't move file to S3 bucket: bucket-{}, sourceKey-{}, destinationKey-{}", bucketName, sourceKey, destinationKey);
            throw new AWSException("Can't move file to S3 bucket: bucket-" + bucketName + ", sourceKey-" + sourceKey + ", destinationKey-" + destinationKey);
        }
    }

}
