package eu.europeana.metis.reprocessing.utilities;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class PropertiesHolderExtension {

  //Mongo Cache
  public final String[] cacheMongoHosts;
  public final int[] cacheMongoPorts;
  public final String cacheMongoAuthenticationDb;
  public final String cacheMongoUsername;
  public final String cacheMongoPassword;
  public final boolean cacheMongoEnablessl;
  public final String cacheMongoDb;

  //S3
  public final String s3AccessKey;
  public final String s3SecretKey;
  public final String s3Endpoint;
  public final String s3Bucket;

  PropertiesHolderExtension(Properties properties) {
    //Mongo Cache
    cacheMongoHosts = properties.getProperty("mongo.cache.hosts").split(",");
    cacheMongoPorts = Arrays.stream(properties.getProperty("mongo.cache.port").split(","))
        .mapToInt(Integer::parseInt).toArray();
    cacheMongoAuthenticationDb = properties.getProperty("mongo.cache.authentication.db");
    cacheMongoUsername = properties.getProperty("mongo.cache.username");
    cacheMongoPassword = properties.getProperty("mongo.cache.password");
    cacheMongoEnablessl = Boolean.parseBoolean(properties.getProperty("mongo.cache.enableSSL"));
    cacheMongoDb = properties.getProperty("mongo.cache.db");

    //S3
    s3AccessKey = properties.getProperty("s3.access.key");
    s3SecretKey = properties.getProperty("s3.secret.key");
    s3Endpoint = properties.getProperty("s3.endpoint");
    s3Bucket = properties.getProperty("s3.bucket");
  }
}
