package eu.europeana.metis.reprocessing.model;

import eu.europeana.metis.reprocessing.dao.MetisCoreMongoDao;
import eu.europeana.metis.reprocessing.dao.MongoDestinationMongoDao;
import eu.europeana.metis.reprocessing.dao.MongoSourceMongoDao;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class BasicConfiguration {

  private final MetisCoreMongoDao metisCoreMongoDao;
  private final MongoSourceMongoDao mongoSourceMongoDao;
  private final MongoDestinationMongoDao mongoDestinationMongoDao;
  private ExtraConfiguration extraConfiguration;

  public BasicConfiguration(PropertiesHolder propertiesHolder)
      throws TrustStoreConfigurationException {
    metisCoreMongoDao = new MetisCoreMongoDao(propertiesHolder);
    mongoSourceMongoDao = new MongoSourceMongoDao(propertiesHolder);
    mongoDestinationMongoDao = new MongoDestinationMongoDao(propertiesHolder);
  }

  public MetisCoreMongoDao getMetisCoreMongoDao() {
    return metisCoreMongoDao;
  }

  public MongoSourceMongoDao getMongoSourceMongoDao() {
    return mongoSourceMongoDao;
  }

  public MongoDestinationMongoDao getMongoDestinationMongoDao() {
    return mongoDestinationMongoDao;
  }

  public ExtraConfiguration getExtraConfiguration() {
    return extraConfiguration;
  }

  public void setExtraConfiguration(
      ExtraConfiguration extraConfiguration) {
    this.extraConfiguration = extraConfiguration;
  }

  void close() {
    metisCoreMongoDao.close();
    mongoSourceMongoDao.close();
    mongoDestinationMongoDao.close();
    if (extraConfiguration != null) {
      extraConfiguration.close();
    }
  }
}
