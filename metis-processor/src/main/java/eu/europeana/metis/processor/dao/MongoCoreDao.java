package eu.europeana.metis.processor.dao;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCompressor;
import com.mongodb.client.MongoClient;
import com.mongodb.connection.SocketSettings;
import dev.morphia.Datastore;
import dev.morphia.query.Query;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.properties.mongo.MongoCoreProperties;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Data access object for the Metis core Mongo.
 */
public class MongoCoreDao {

  private final MongoCoreProperties mongoCoreProperties;
  private final MongoClient mongoClient;
  private final Datastore metisCoreDatastore;

  public MongoCoreDao(MongoCoreProperties mongoCoreProperties) throws DataAccessConfigException {
    this.mongoCoreProperties = mongoCoreProperties;
    this.mongoClient = initializeMongoClient();
    this.metisCoreDatastore = initializeDatastore();
  }

  public List<String> getAllDatasetIds() {
    Query<Dataset> query = metisCoreDatastore.find(Dataset.class);
    final List<Dataset> datasets = MorphiaUtils.getListOfQueryRetryable(query);
    return datasets.stream().map(Dataset::getDatasetId).toList();
  }

  private MongoClient initializeMongoClient() throws DataAccessConfigException {
    // Configure SocketSettings with a 16 MB buffer for send and receive
    SocketSettings socketSettings = SocketSettings.builder()
                                                  .receiveBufferSize(16 * 1024 * 1024) // 16 MB buffer for receiving
                                                  .sendBufferSize(16 * 1024 * 1024)    // 16 MB buffer for sending
                                                  .connectTimeout(30, TimeUnit.SECONDS) // Connection timeout
                                                  .readTimeout(30, TimeUnit.SECONDS)    // Read timeout
                                                  .build();
    MongoClientSettings.Builder settings = MongoClientSettings.builder()
                                                              .applyToSocketSettings(
                                                                  builder -> builder.applySettings(socketSettings))
                                                              .compressorList(
                                                                  Arrays.asList(MongoCompressor.createSnappyCompressor(),
                                                                      MongoCompressor.createZlibCompressor(),
                                                                      MongoCompressor.createZstdCompressor()));
    return new MongoClientProvider<>(mongoCoreProperties.getMongoCoreProperties(), settings).createMongoClient();
  }

  private Datastore initializeDatastore() {
    return new MorphiaDatastoreProviderImpl(mongoClient, mongoCoreProperties.getMongoCoreDatabase()).getDatastore();
  }

}
