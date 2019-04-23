package eu.europeana.metis.remove.discover;

import com.mongodb.DBCollection;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutionProgress;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.utils.ExternalRequestUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.mongodb.morphia.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as an engine for discovering orphans. Individual cleanup actions can extend this
 * class and provide the functionality to identify the orphans given an instance of {@link
 * ExecutionPluginForest}.
 */
abstract class AbstractOrphanIdentification {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOrphanIdentification.class);

  private final MorphiaDatastoreProvider morphiaDatastoreProvider;

  private final DiscoveryMode discoveryMode;

  enum DiscoveryMode {

    /**
     * This mode removes all orphans and their descendants.
     */
    DISCOVER_ALL_ORPHANS(node -> true),

    /**
     * This mode removes only orphans or their descendants that have no children.
     */
    DISCOVER_ONLY_CHILDLESS_ORPHANS(node -> node.getChildren().isEmpty());

    private final Predicate<ExecutionPluginNode> test;

    DiscoveryMode(Predicate<ExecutionPluginNode> test) {
      this.test = test;
    }
  }

  /**
   * Constructor.
   *
   * @param morphiaDatastoreProvider Access to the database.
   * @param discoveryMode The discoveryMode in which to operate.
   */
  AbstractOrphanIdentification(MorphiaDatastoreProvider morphiaDatastoreProvider, DiscoveryMode discoveryMode) {
    this.morphiaDatastoreProvider = morphiaDatastoreProvider;
    this.discoveryMode = discoveryMode;
  }

  /**
   * This method computes all remove iterations. This means that it repeatedly compute the nodes to
   * remove, and simulates removing these nodes before computing the next batch. This continue until
   * no more nodes can be removed. It prints the iterations to the log, and returns only the nodes
   * to remove for the FIRST iteration (i.e. the nodes that can currently be removed).
   *
   * @return The nodes that can currently be removed.
   */
  final List<ExecutionPluginNode> discoverOrphans() {

    // Compute all iterations
    final List<List<ExecutionPluginNode>> iterations = new ArrayList<>();
    final Set<String> datasetIds = getDatasetIds();
    final Set<String> datasetIdsToSkip = new HashSet<>();
    final Set<String> nodeIdsToSkip = new HashSet<>();
    while (true) {

      // Perform one iteration. Compute which nodes to remove for this iteration.
      final List<ExecutionPluginNode> nodesToRemove = new ArrayList<>();
      for (String datasetId : datasetIds) {

        // If we can skip this dataset, do so.
        if (datasetIdsToSkip.contains(datasetId)) {
          continue;
        }

        // Obtain the orphans for this dataset. If we find none, we mark this dataset to skip.
        final List<ExecutionPluginNode> orphans = discoverOrphans(datasetId, nodeIdsToSkip);
        if (orphans.isEmpty()) {
          datasetIdsToSkip.add(datasetId);
        }

        // Add orphans and their descendants in the right order to the list for removal.
        orphans.stream().map(ExecutionPluginNode::getAllInOrderOfRemoval).flatMap(List::stream)
            .filter(discoveryMode.test).forEach(nodesToRemove::add);
      }

      // If we have no more nodes to remove, we are done.
      if (nodesToRemove.isEmpty()) {
        break;
      }

      // Save the nodes to remove as an iteration.
      iterations.add(nodesToRemove);

      // Add all nodes to remove to the node skip list: we can ignore them in the next iteration.
      nodesToRemove.stream().map(ExecutionPluginNode::getId).forEach(nodeIdsToSkip::add);
    }

    // If there is nothing to do we print a message and we are done.
    if (iterations.isEmpty()) {
      LOGGER.info("Nothing to do: no orphans/leaf nodes found.");
      return Collections.emptyList();
    }

    // Print all iterations to the log.
    final ToIntFunction<ExecutionPluginNode> recordCounter = node -> Optional
        .ofNullable(node.getPlugin()).map(AbstractMetisPlugin::getExecutionProgress)
        .map(ExecutionProgress::getProcessedRecords).orElse(0);
    LOGGER.info("{} iterations are needed:", iterations.size());
    for (int i = 0; i < iterations.size(); i++) {
      final List<ExecutionPluginNode> nodesToRemove = iterations.get(i);
      final long linkCheckingExecutions = nodesToRemove.stream().map(ExecutionPluginNode::getType)
          .filter(type -> type == PluginType.LINK_CHECKING).count();
      LOGGER.info(
          " ... Iteration {}: remove {} nodes ({} records). {} of these are link checking executions.",
          i, nodesToRemove.size(), nodesToRemove.stream().mapToInt(recordCounter).sum(),
          linkCheckingExecutions);
    }

    // Return just the first iteration.
    return iterations.get(0);
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

    // Identify the orphans
    return identifyOrphans(forest);
  }

  abstract List<ExecutionPluginNode> identifyOrphans(ExecutionPluginForest forest);

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
