package eu.europeana.metis.reprocessing.model;

import eu.europeana.metis.reprocessing.utilities.MongoDao;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class BasicConfiguration {

  private final MongoDao mongoDao;
  private ExtraConfiguration extraConfiguration;

  public BasicConfiguration(PropertiesHolderExtension propertiesHolderExtension)
      throws TrustStoreConfigurationException {
    this.mongoDao = new MongoDao(propertiesHolderExtension);
  }

  public MongoDao getMongoDao() {
    return mongoDao;
  }

  public ExtraConfiguration getExtraConfiguration() {
    return extraConfiguration;
  }

  public void setExtraConfiguration(
      ExtraConfiguration extraConfiguration) {
    this.extraConfiguration = extraConfiguration;
  }

  void close() {
    mongoDao.close();
    if (extraConfiguration != null)
      extraConfiguration.close();
  }
}
