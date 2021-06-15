package eu.europeana.metis_tools.inc_harvest.data;

import static eu.europeana.metis.core.common.DaoFieldNames.DATASET_ID;
import static eu.europeana.metis.core.common.DaoFieldNames.ID;

import dev.morphia.aggregation.experimental.Aggregation;
import dev.morphia.aggregation.experimental.expressions.Expressions;
import dev.morphia.aggregation.experimental.stages.Projection;
import dev.morphia.annotations.Entity;
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

      // Create aggregation pipeline finding all datasets.
      final Aggregation<Dataset> pipeline = datastoreProvider
              .getDatastore().aggregate(eu.europeana.metis.core.dataset.Dataset.class);

      // The field name should be the field name in DatasetIdWrapper.
      final String datasetIdField = "datasetId";

      // Project the dataset ID to the right field name.
      pipeline.project(Projection.project().exclude(ID.getFieldName())
              .include(datasetIdField, Expressions.field(DATASET_ID.getFieldName())));

      // Perform the aggregation and add the IDs in the result set.
      final Iterator<DatasetIdWrapper> resultIterator = pipeline.execute(DatasetIdWrapper.class);
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, 0), false)
              .map(DatasetIdWrapper::getDatasetId);
    });
  }

  @Entity
  private static class DatasetIdWrapper {

    // Name depends on the mongo aggregations in which it is used.
    private String datasetId;

    String getDatasetId() {
      return datasetId;
    }
  }
}
