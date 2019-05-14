package eu.europeana.metis.reprocessing.utilities;

import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.metis.core.dataset.Dataset;
import eu.europeana.metis.core.workflow.OrderField;
import eu.europeana.metis.reprocessing.model.DatasetStatus;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.query.FindOptions;
import org.mongodb.morphia.query.Query;

/**
 * Mongo functionality required for the current script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class MongoDao {

  private static final String DATASET_ID = "datasetId";
  private static final int PAGE_SIZE = 200;

  private Datastore metisCoreDatastore;
  private Datastore mongoSourceDatastore;
  private Datastore mongoDestinationDatastore;

  MongoDao(Datastore metisCoreDatastore, Datastore mongoSourceDatastore,
      Datastore mongoDestinationDatastore) {
    this.metisCoreDatastore = metisCoreDatastore;
    this.mongoSourceDatastore = mongoSourceDatastore;
    this.mongoDestinationDatastore = mongoDestinationDatastore;
  }

  public List<String> getAllDatasetIdsOrdered() {
    Query<Dataset> query = metisCoreDatastore.createQuery(Dataset.class);
    //Order by dataset id which is a String order not a number order.
    query.order(DATASET_ID);
    final List<Dataset> datasets = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(query::asList);
    return datasets.stream().map(Dataset::getDatasetId).collect(Collectors.toList());
  }

  public List<FullBeanImpl> getNextPageOfRecords(String datasetId, int nextPage) {
    Query<FullBeanImpl> query = mongoSourceDatastore.createQuery(FullBeanImpl.class);
    query.field("about").startsWith("/" + datasetId + "/");
    query.order(OrderField.ID.getOrderFieldName());
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(() -> query.asList(
        new FindOptions().skip(nextPage * PAGE_SIZE).limit(PAGE_SIZE)));
  }

  DatasetStatus getDatasetStatus(String datasetId) {
    return mongoDestinationDatastore.find(DatasetStatus.class).filter(DATASET_ID, datasetId).get();
  }

  void storeDatasetStatusToDb(DatasetStatus datasetStatus) {
    mongoDestinationDatastore.save(datasetStatus);
  }


}
