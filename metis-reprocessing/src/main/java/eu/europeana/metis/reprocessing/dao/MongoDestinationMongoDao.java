package eu.europeana.metis.reprocessing.dao;

import com.mongodb.MongoClient;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolderExtension;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class MongoDestinationMongoDao {

  private static final String DATASET_ID = "datasetId";

  private final MongoInitializer mongoDestinationMongoInitializer;
  private Datastore mongoDestinationDatastore;
  private PropertiesHolderExtension propertiesHolderExtension;

  public MongoDestinationMongoDao(PropertiesHolderExtension propertiesHolderExtension) {
    this.propertiesHolderExtension = propertiesHolderExtension;
    //Mongo Destination
    mongoDestinationMongoInitializer = prepareMongoDestinationConfiguration();
    mongoDestinationDatastore = createMongoDestinationDatastore(
        mongoDestinationMongoInitializer.getMongoClient(), propertiesHolderExtension.sourceMongoDb);
  }

  public DatasetStatus getDatasetStatus(String datasetId) {
    return mongoDestinationDatastore.find(DatasetStatus.class).filter(DATASET_ID, datasetId).get();
  }

  public void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
    mongoDestinationDatastore.save(datasetStatus);
  }

  private MongoInitializer prepareMongoDestinationConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolderExtension.destinationMongoHosts,
        propertiesHolderExtension.destinationMongoPorts,
        propertiesHolderExtension.destinationMongoAuthenticationDb,
        propertiesHolderExtension.destinationMongoUsername,
        propertiesHolderExtension.destinationMongoPassword,
        propertiesHolderExtension.destinationMongoEnablessl,
        propertiesHolderExtension.destinationMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMongoDestinationDatastore(MongoClient mongoClient,
      String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(DatasetStatus.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    //Ensure indexes, to create them in destination only
    datastore.ensureIndexes();
    return datastore;
  }

  public void close() {
    mongoDestinationMongoInitializer.close();
  }

}
