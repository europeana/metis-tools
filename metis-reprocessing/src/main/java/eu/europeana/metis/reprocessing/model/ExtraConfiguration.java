package eu.europeana.metis.reprocessing.model;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import eu.europeana.corelib.definitions.jibx.RDF;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.reprocessing.dao.CacheMongoDao;
import eu.europeana.metis.reprocessing.exception.ProcessingException;
import eu.europeana.metis.reprocessing.execution.IndexingUtilities;
import eu.europeana.metis.reprocessing.execution.ProcessingUtilities;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class ExtraConfiguration {

  private final CacheMongoDao cacheMongoDao;
  private final AmazonS3 amazonS3Client;
  private final String s3Bucket;
  private final ThrowingBiFunction<FullBeanImpl, BasicConfiguration, RDF> fullBeanProcessor;
  private final ThrowingTriConsumer<RDF, Boolean, BasicConfiguration> rdfIndexer;

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
    this.rdfIndexer = IndexingUtilities::indexRdf;
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

  public ThrowingBiFunction<FullBeanImpl, BasicConfiguration, RDF> getFullBeanProcessor() {
    return fullBeanProcessor;
  }

  public ThrowingTriConsumer<RDF, Boolean, BasicConfiguration> getRdfIndexer() {
    return rdfIndexer;
  }

  void close() {
    cacheMongoDao.close();
  }

  @FunctionalInterface
  public interface ThrowingBiFunction<T, U, R> {

    R apply(T t, U u) throws ProcessingException;
  }

  @FunctionalInterface
  public interface ThrowingTriConsumer<K, V, S> {

    void accept(K k, V v, S s) throws IndexingException;
  }
}
