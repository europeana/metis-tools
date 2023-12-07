package eu.europeana.metis.processor.utilities;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;

/**
 * The type Amazon client.
 */
public class S3Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final AmazonS3 ibmAmazonS3;
    private final String ibmS3BucketName;
    private final AmazonS3 amazonS3;
    private final String amazonS3BucketName;

    /**
     * Instantiates a new Amazon client.
     *
     * @param ibmAmazonS3  the amazon s3
     * @param ibmS3BucketName the aws bucket
     */
    public S3Client(AmazonS3 ibmAmazonS3, String ibmS3BucketName, AmazonS3 amazonS3, String amazonS3BucketName) {
        this.ibmAmazonS3 = ibmAmazonS3;
        this.ibmS3BucketName = ibmS3BucketName;
        this.amazonS3 = amazonS3;
        this.amazonS3BucketName = amazonS3BucketName;
    }

    /**
     * Store object in the default bucket specified during creation of the client
     *
     * @param awsObjectName  object name
     * @param inputStream    object content
     * @param objectMetadata object metadata
     * @return result from AmazonS3
     */
    public PutObjectResult putIbmObject(String awsObjectName, InputStream inputStream, ObjectMetadata objectMetadata) {
        return ibmAmazonS3.putObject(ibmS3BucketName, awsObjectName, inputStream, objectMetadata);
    }


    /**
     * Gets object from ibm
     *
     * @param awsObjectName the name
     * @return the object
     */
    public byte[] getIbmObject(String awsObjectName) {
        return getObject(ibmAmazonS3, ibmS3BucketName, awsObjectName);
    }

    /**
     * Gets object from amazon
     *
     * @param awsObjectName the name
     * @return the object
     */
    public byte[] getAmazonObject(String awsObjectName) {
        return getObject(amazonS3, amazonS3BucketName, awsObjectName);
    }

    private byte[] getObject(AmazonS3 s3, String bucketName, String awsObjectName) {
        try {
            final S3Object s3Object = s3.getObject(bucketName, awsObjectName);
            final S3ObjectInputStream s3is = s3Object.getObjectContent();
            ByteArrayOutputStream fos = new ByteArrayOutputStream();
            final byte[] readBuffer = new byte[1024];
            int readLength;
            while ((readLength = s3is.read(readBuffer)) > 0) {
                fos.write(readBuffer, 0, readLength);
            }
            s3is.close();
            return fos.toByteArray();
        } catch (AmazonServiceException | IOException e) {
            LOGGER.error("occurred in getting object", e);
            return new byte[0];
        }
    }
}
