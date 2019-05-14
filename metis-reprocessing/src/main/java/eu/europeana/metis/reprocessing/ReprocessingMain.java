package eu.europeana.metis.reprocessing;

import static eu.europeana.metis.reprocessing.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import com.mongodb.MongoClient;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.corelib.solr.entity.AgentImpl;
import eu.europeana.corelib.solr.entity.AggregationImpl;
import eu.europeana.corelib.solr.entity.BasicProxyImpl;
import eu.europeana.corelib.solr.entity.ConceptImpl;
import eu.europeana.corelib.solr.entity.ConceptSchemeImpl;
import eu.europeana.corelib.solr.entity.EuropeanaAggregationImpl;
import eu.europeana.corelib.solr.entity.EventImpl;
import eu.europeana.corelib.solr.entity.PhysicalThingImpl;
import eu.europeana.corelib.solr.entity.PlaceImpl;
import eu.europeana.corelib.solr.entity.ProvidedCHOImpl;
import eu.europeana.corelib.solr.entity.ProxyImpl;
import eu.europeana.corelib.solr.entity.TimespanImpl;
import eu.europeana.corelib.solr.entity.WebResourceImpl;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.utilities.ExecutorManager;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-14
 */
public class ReprocessingMain {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ReprocessingMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args) throws TrustStoreConfigurationException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Starting script");

    //Metis Core Datastore
    final MongoInitializer metisCoreMongoInitializer = prepareMetisCoreConfiguration();
    final Datastore metisCoreDatastore = createMetisCoreDatastore(
        metisCoreMongoInitializer.getMongoClient(),
        propertiesHolder.metisCoreMongoDb);

    //Mongo source
    final MongoInitializer mongoSourceMongoInitializer = prepareMongoSourceConfiguration();
    final Datastore mongoSourceDatastore = createMongoSourceDatastore(
        mongoSourceMongoInitializer.getMongoClient(), propertiesHolder.sourceMongoDb);

    //Mongo destination
    final MongoInitializer mongoDestinationMongoInitializer = prepareMongoDestinationConfiguration();
    final Datastore mongoDestinationDatastore = createMongoDestinationDatastore(
        mongoDestinationMongoInitializer.getMongoClient(), propertiesHolder.sourceMongoDb);

    final ExecutorManager executorManager = new ExecutorManager(metisCoreDatastore,
        mongoSourceDatastore, mongoDestinationDatastore);
    executorManager.startReprocessing();

    executorManager.close();
    metisCoreMongoInitializer.close();
    mongoSourceMongoInitializer.close();
    mongoDestinationMongoInitializer.close();
  }

  private static MongoInitializer prepareMetisCoreConfiguration()
      throws TrustStoreConfigurationException {
    if (StringUtils.isNotEmpty(propertiesHolder.truststorePath) && StringUtils
        .isNotEmpty(propertiesHolder.truststorePassword)) {
      LOGGER.info(EXECUTION_LOGS_MARKER,
          "Append default truststore with custom truststore");
      CustomTruststoreAppender.appendCustomTrustoreToDefault(propertiesHolder.truststorePath,
          propertiesHolder.truststorePassword);
    }
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.metisCoreMongoHosts,
        propertiesHolder.metisCoreMongoPorts, propertiesHolder.metisCoreMongoAuthenticationDb,
        propertiesHolder.metisCoreMongoUsername, propertiesHolder.metisCoreMongoPassword,
        propertiesHolder.metisCoreMongoEnablessl, propertiesHolder.metisCoreMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static MongoInitializer prepareMongoSourceConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.sourceMongoHosts,
        propertiesHolder.sourceMongoPorts, propertiesHolder.sourceMongoAuthenticationDb,
        propertiesHolder.sourceMongoUsername, propertiesHolder.sourceMongoPassword,
        propertiesHolder.sourceMongoEnablessl, propertiesHolder.sourceMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static MongoInitializer prepareMongoDestinationConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(propertiesHolder.destinationMongoHosts,
        propertiesHolder.destinationMongoPorts, propertiesHolder.destinationMongoAuthenticationDb,
        propertiesHolder.destinationMongoUsername, propertiesHolder.destinationMongoPassword,
        propertiesHolder.destinationMongoEnablessl, propertiesHolder.destinationMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMetisCoreDatastore(MongoClient mongoClient, String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(Dataset.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    datastore.ensureIndexes();
    return datastore;
  }

  private static Datastore createMongoSourceDatastore(MongoClient mongoClient,
      String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(FullBeanImpl.class);
    morphia.map(ProvidedCHOImpl.class);
    morphia.map(AgentImpl.class);
    morphia.map(AggregationImpl.class);
    morphia.map(ConceptImpl.class);
    morphia.map(ProxyImpl.class);
    morphia.map(PlaceImpl.class);
    morphia.map(TimespanImpl.class);
    morphia.map(WebResourceImpl.class);
    morphia.map(EuropeanaAggregationImpl.class);
    morphia.map(EventImpl.class);
    morphia.map(PhysicalThingImpl.class);
    morphia.map(ConceptSchemeImpl.class);
    morphia.map(BasicProxyImpl.class);
    return morphia.createDatastore(mongoClient, databaseName);
  }

  private static Datastore createMongoDestinationDatastore(MongoClient mongoClient,
      String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(FullBeanImpl.class);
    morphia.map(ProvidedCHOImpl.class);
    morphia.map(AgentImpl.class);
    morphia.map(AggregationImpl.class);
    morphia.map(ConceptImpl.class);
    morphia.map(ProxyImpl.class);
    morphia.map(PlaceImpl.class);
    morphia.map(TimespanImpl.class);
    morphia.map(WebResourceImpl.class);
    morphia.map(EuropeanaAggregationImpl.class);
    morphia.map(EventImpl.class);
    morphia.map(PhysicalThingImpl.class);
    morphia.map(ConceptSchemeImpl.class);
    morphia.map(BasicProxyImpl.class);

    morphia.map(DatasetStatus.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    //Ensure indexes, to create them in destination only
    datastore.ensureIndexes();
    return datastore;
  }
}
