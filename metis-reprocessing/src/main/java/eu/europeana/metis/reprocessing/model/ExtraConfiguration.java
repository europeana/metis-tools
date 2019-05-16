package eu.europeana.metis.reprocessing.model;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.reprocessing.execution.ProcessingUtilities;
import eu.europeana.metis.reprocessing.dao.CacheMongoDao;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import org.apache.logging.log4j.util.BiConsumer;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class ExtraConfiguration {

  private final CacheMongoDao cacheMongoDao;
  private final AmazonS3 amazonS3Client;
  private final String s3Bucket;
  final BiConsumer<FullBeanImpl, BasicConfiguration> fullBeanProcessor;

  public ExtraConfiguration(PropertiesHolderExtension propertiesHolderExtension) {
    this.cacheMongoDao = new CacheMongoDao(propertiesHolderExtension);

    //S3
    this.amazonS3Client = new AmazonS3Client(new BasicAWSCredentials(
        propertiesHolderExtension.s3AccessKey,
        propertiesHolderExtension.s3SecretKey));
    amazonS3Client.setEndpoint(propertiesHolderExtension.s3Endpoint);
    this.s3Bucket = propertiesHolderExtension.s3Bucket;

    this.fullBeanProcessor = ProcessingUtilities::updateTechnicalMetadata;
  }

  public CacheMongoDao getCacheMongoDao() {
    return cacheMongoDao;
  }

  public AmazonS3 getAmazonS3Client() {
    return amazonS3Client;
  }

  public String getS3Bucket() {
    return s3Bucket;
  }

  public BiConsumer<FullBeanImpl, BasicConfiguration> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  void close() {
    cacheMongoDao.close();
  }
}
