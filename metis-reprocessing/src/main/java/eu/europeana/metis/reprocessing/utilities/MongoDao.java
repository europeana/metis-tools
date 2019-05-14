package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.Query;

/**
 * Mongo functionality required for the current script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class MongoDao {

  private static final String DATASET_ID = "datasetId";

  private Datastore datastore;

  MongoDao(Datastore datastore) {
    this.datastore = datastore;
  }

  List<String> getAllDatasetIdsOrdered() {
    Query<Dataset> query = datastore.createQuery(Dataset.class);
    //Order by dataset id which is a String order not a number order.
    query.order(DATASET_ID);
    final List<Dataset> datasets = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(query::asList);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }
}
