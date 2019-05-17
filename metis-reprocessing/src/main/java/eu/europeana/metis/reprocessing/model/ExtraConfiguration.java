package eu.europeana.metis.reprocessing.model;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.reprocessing.dao.CacheMongoDao;
import eu.europeana.metis.reprocessing.execution.ProcessingUtilities;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class ExtraConfiguration {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExtraConfiguration.class);

  private final CacheMongoDao cacheMongoDao;
  private final AmazonS3 amazonS3Client;
  private final String s3Bucket;
  private final BiFunction<FullBeanImpl, BasicConfiguration, RDF> fullBeanProcessor;

  public ExtraConfiguration(PropertiesHolder propertiesHolder) {
    final PropertiesHolderExtension propertiesHolderExtension = propertiesHolder
        .getPropertiesHolderExtension();
    this.cacheMongoDao = new CacheMongoDao(propertiesHolder);

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

  public BiFunction<FullBeanImpl, BasicConfiguration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  void close() {
    cacheMongoDao.close();
  }
}
