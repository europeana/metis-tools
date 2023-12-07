package eu.europeana.metis.processor.properties.general;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class IbmS3Properties {

    @Value("${ibm.s3.endpoint}")
    private String s3Endpoint;
    @Value("${ibm.s3.bucket.name}")
    private String s3BucketName;
    @Value("${ibm.s3.access.key}")
    private String s3AccessKey;
    @Value("${ibm.s3.secret.key}")
    private String s3SecretKey;

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3BucketName() {
        return s3BucketName;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }
}
