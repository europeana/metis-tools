package eu.europeana.metis.remove.discover;

import com.mongodb.DBCollection;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutionProgress;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.ToIntFunction;
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

    // Compute all iterations
    final List<List<ExecutionPluginNode>> iterations = new ArrayList<>();
    final Set<String> idsToSkip = new HashSet<>();
    while (true) {
      final List<ExecutionPluginNode> nodesToRemove = new ArrayList<>();
      getDatasetIds().stream().map(id -> discoverOrphans(id, idsToSkip)).flatMap(List::stream)
          .map(ExecutionPluginNode::getAllInOrderOfRemoval).forEach(nodesToRemove::addAll);
      if (nodesToRemove.isEmpty()) {
        break;
      }
      nodesToRemove.stream().map(ExecutionPluginNode::getId).forEach(idsToSkip::add);
      iterations.add(nodesToRemove);
    }

    // Print all iterations to the log.
    final ToIntFunction<ExecutionPluginNode> recordCounter = node -> Optional
        .ofNullable(node.getPlugin()).map(AbstractMetisPlugin::getExecutionProgress)
        .map(ExecutionProgress::getProcessedRecords).orElse(0);
    LOGGER.info("{} iterations are needed:", iterations.size());
    for (int i = 0; i < iterations.size(); i++) {
      final List<ExecutionPluginNode> nodesToRemove = iterations.get(i);
      final long linkCheckingExecutions = nodesToRemove.stream().map(ExecutionPluginNode::getPlugin)
          .map(AbstractMetisPlugin::getPluginType).filter(type -> type == PluginType.LINK_CHECKING)
          .count();
      LOGGER.info(
          " ... Iteration {}: remove {} nodes ({} records). {} of these are link checking executions.",
          i, nodesToRemove.size(), nodesToRemove.stream().mapToInt(recordCounter).sum(),
          linkCheckingExecutions);
    }

    // Return just the result of the first iteration.
    if (!iterations.isEmpty()) {
      saveOrphansToFile(iterations.get(0));
    }
  }

  private void saveOrphansToFile(List<ExecutionPluginNode> nodesToRemove) {

  }

  private List<ExecutionPluginNode> discoverOrphans(String datasetId, Set<String> idsToSkip) {

    // Creating the forest
    final ExecutionPluginForest forest;
    try {
      forest = new ExecutionPluginForest(getWorkflowExecutions(datasetId), idsToSkip);
    } catch (RuntimeException e) {
      LOGGER.warn("Problem with dataset {}.", datasetId, e);
      return Collections.emptyList();
    }

    // Analyze the forest
    final List<ExecutionPluginNode> result = new ArrayList<>();
    final Instant cutoffDate = LocalDateTime.of(2018, Month.OCTOBER, 13, 0, 0)
        .atZone(ZoneId.systemDefault()).toInstant();
    result.addAll(forest.getFailedOrCancelledLeafs(cutoffDate));
    result.addAll(forest.getFinishedSupersededLeafs(cutoffDate));
    return result;
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
