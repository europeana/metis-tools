package eu.europeana.metis.processor.dao;

import com.mongodb.client.MongoClient;
import dev.morphia.Datastore;
import dev.morphia.query.Query;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.processor.config.DataAccessConfigException;
import eu.europeana.metis.processor.config.mongo.MongoCoreProperties;

import java.util.List;
import java.util.stream.Collectors;

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

  private MongoClient initializeMongoClient() throws DataAccessConfigException {
    return new MongoClientProvider<>(mongoCoreProperties.getMongoCoreProperties()).createMongoClient();
  }

  private Datastore initializeDatastore() {
    return new MorphiaDatastoreProviderImpl(mongoClient, mongoCoreProperties.getMongoCoreDatabase()).getDatastore();
  }


  public List<String> getAllDatasetIds() {
    Query<Dataset> query = metisCoreDatastore.find(Dataset.class);
    final List<Dataset> datasets = MorphiaUtils.getListOfQueryRetryable(query);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }

}
