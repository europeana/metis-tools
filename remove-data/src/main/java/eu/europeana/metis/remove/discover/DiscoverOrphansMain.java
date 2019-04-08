package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.remove.utils.MongoInitializer;
import eu.europeana.metis.remove.utils.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiscoverOrphansMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoverOrphansMain.class);

  private static final String OUTPUT_FILE = "/home/jochen/Documents/orphans.csv";

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {

    final PropertiesHolder propertiesHolder = new PropertiesHolder();

    LOGGER.info("Append default truststore with custom truststore");
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }

    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder);
    mongoInitializer.initializeMongoClient();
    MorphiaDatastoreProvider morphiaDatastoreProvider = new MorphiaDatastoreProvider(
        mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);

    final DiscoverOrphans discoverOrphans = new DiscoverOrphans(morphiaDatastoreProvider);

    discoverOrphans.discoverOrphans(OUTPUT_FILE);

    mongoInitializer.close();
  }
}
