package eu.europeana.metis.reprocessing.dao;

import com.mongodb.MongoClient;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.reprocessing.model.FailedRecord;
import eu.europeana.metis.reprocessing.utilities.MongoInitializer;
import eu.europeana.metis.reprocessing.utilities.PropertiesHolder;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.List;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;

/**
 * Mongo Dao for destination mongo.
 * <p>This is where the new records will reside as well as progress information of datasets and
 * failed records, see
 * {@link DatasetStatus} and {@link FailedRecord}</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-05-16
 */
public class MongoDestinationMongoDao {

  private static final String DATASET_ID = "datasetId";
  private static final String FAILED_URL = "failedUrl";

  private final MongoInitializer mongoDestinationMongoInitializer;
  private Datastore mongoDestinationDatastore;
  private PropertiesHolder propertiesHolder;

  public MongoDestinationMongoDao(PropertiesHolder propertiesHolder) {
    this.propertiesHolder = propertiesHolder;
    //Mongo Destination
    mongoDestinationMongoInitializer = prepareMongoDestinationConfiguration();
    mongoDestinationDatastore = createMongoDestinationDatastore(
        mongoDestinationMongoInitializer.getMongoClient(), propertiesHolder.sourceMongoDb);
  }

  public List<FailedRecord> getNextPageOfFailedRecords(String datasetId, int nextPage) {
    Query<FailedRecord> query = mongoDestinationDatastore.createQuery(FailedRecord.class);
    query.field(FAILED_URL).startsWith("/" + datasetId + "/");
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(() -> query.asList(
        new FindOptions().skip(nextPage * MongoSourceMongoDao.PAGE_SIZE)
            .limit(MongoSourceMongoDao.PAGE_SIZE)));
  }

  public DatasetStatus getDatasetStatus(String datasetId) {
    return mongoDestinationDatastore.find(DatasetStatus.class).filter(DATASET_ID, datasetId).get();
  }

  public void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
    mongoDestinationDatastore.save(datasetStatus);
  }

  public void storeFailedRecordToDb(FailedRecord failedRecord) {
    //Will replace it if already existent
    mongoDestinationDatastore.save(failedRecord);
  }

  public void deleteFailedRecord(FailedRecord failedRecord) {
    mongoDestinationDatastore.delete(failedRecord);
  }

  private MongoInitializer prepareMongoDestinationConfiguration() {
    MongoInitializer mongoInitializer = new MongoInitializer(
        propertiesHolder.destinationMongoHosts,
        propertiesHolder.destinationMongoPorts,
        propertiesHolder.destinationMongoAuthenticationDb,
        propertiesHolder.destinationMongoUsername,
        propertiesHolder.destinationMongoPassword,
        propertiesHolder.destinationMongoEnablessl,
        propertiesHolder.destinationMongoDb);
    mongoInitializer.initializeMongoClient();
    return mongoInitializer;
  }

  private static Datastore createMongoDestinationDatastore(MongoClient mongoClient,
      String databaseName) {
    Morphia morphia = new Morphia();
    morphia.map(DatasetStatus.class);
    morphia.map(FailedRecord.class);
    final Datastore datastore = morphia.createDatastore(mongoClient, databaseName);
    //Ensure indexes, to create them in destination only
    datastore.ensureIndexes();
    return datastore;
  }

  public void close() {
    mongoDestinationMongoInitializer.close();
  }

}
