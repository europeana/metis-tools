package eu.europeana.metis_tools.inc_harvest.data;

import static eu.europeana.metis.core.common.DaoFieldNames.DATASET_ID;
import static eu.europeana.metis.core.common.DaoFieldNames.ID;

import dev.morphia.query.FindOptions;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProviderImpl;
import eu.europeana.metis.mongo.connection.MongoClientProvider;
import eu.europeana.metis.network.ExternalRequestUtil;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoCoreDao {

  private final MorphiaDatastoreProvider datastoreProvider;

  public MongoCoreDao(MongoClientProvider<IllegalArgumentException> mongoClientProvider,
          String mongoCoreDb) {
    this.datastoreProvider = new MorphiaDatastoreProviderImpl(
            mongoClientProvider.createMongoClient(), mongoCoreDb);
  }

  public Stream<String> getAllDatasetIds() {
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {
      final FindOptions findOptions = new FindOptions()
              .projection().exclude(ID.getFieldName())
              .projection().include(DATASET_ID.getFieldName());
      final Iterator<Dataset> resultIterator = datastoreProvider.getDatastore().find(Dataset.class)
              .iterator(findOptions);
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, 0), false)
              .map(Dataset::getDatasetId);
    });
  }
}
