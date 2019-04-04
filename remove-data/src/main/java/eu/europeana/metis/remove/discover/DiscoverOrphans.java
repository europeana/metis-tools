package eu.europeana.metis.remove.discover;

import com.mongodb.DBCollection;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DiscoverOrphans {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoverOrphans.class);

  private final MorphiaDatastoreProvider morphiaDatastoreProvider;

  DiscoverOrphans(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
  }

  void discoverOrphans() {
    getDatasetIds().forEach(this::discoverOrphans);
  }

  private void discoverOrphans(String datasetId) {
    LOGGER.info("Discovering orphans for dataset {}.", datasetId);
    final ExecutionPluginForest forest;
    try {
      forest = new ExecutionPluginForest(getWorkflowExecutions(datasetId));
    } catch (RuntimeException e) {
      LOGGER.warn(" ... Problem with dataset {}.", datasetId, e);
      return;
    }
    LOGGER.info(" ... {} node(s) found for dataset {}.", forest.getNodeCount(), datasetId);
  }

  private Set<String> getDatasetIds() {
    final DBCollection collection = morphiaDatastoreProvider.getDatastore()
        .getCollection(WorkflowExecution.class);
    final List<?> datasetIds = ExternalRequestUtil
        .retryableExternalRequestConnectionReset(() -> collection.distinct("datasetId"));
    return datasetIds.stream().map(entry -> (String) entry).collect(Collectors.toSet());
  }

  private List<WorkflowExecution> getWorkflowExecutions(String datasetId) {
    final Query<WorkflowExecution> query = morphiaDatastoreProvider.getDatastore()
        .createQuery(WorkflowExecution.class).field("datasetId").equal(datasetId);
    return ExternalRequestUtil.retryableExternalRequestConnectionReset(query::asList);
  }
}
