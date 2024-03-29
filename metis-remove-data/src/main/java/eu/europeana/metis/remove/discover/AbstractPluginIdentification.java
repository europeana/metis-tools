package eu.europeana.metis.remove.discover;

import com.mongodb.client.MongoCollection;
import dev.morphia.query.Query;
import dev.morphia.query.filters.Filters;
import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutionProgress;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.mongo.utils.MorphiaUtils;
import eu.europeana.metis.network.ExternalRequestUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class acts as an engine for discovering plugins. Individual cleanup actions can extend this
 * class and provide the functionality to identify the plugins given an instance of {@link
 * ExecutionPluginForest}.
 */
abstract class AbstractPluginIdentification {

  private final ToIntFunction<ExecutionPluginNode> RECORD_COUNTER = node -> Optional
      .ofNullable(node.getPlugin()).filter(plugin -> plugin instanceof AbstractExecutablePlugin)
      .map(plugin -> (AbstractExecutablePlugin) plugin)
      .map(AbstractExecutablePlugin::getExecutionProgress)
      .map(ExecutionProgress::getProcessedRecords).orElse(0);


  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPluginIdentification.class);

  private final MorphiaDatastoreProvider morphiaDatastoreProvider;

  private final DiscoveryMode discoveryMode;

  enum DiscoveryMode {

    /**
     * This mode discovers only identified plugins or their descendants that have no children. The
     * deleted status of the plugin or its children is not taken into account. Note: in case that
     * we're removing plugins, we should only use this mode if we intend to completely remove
     * plugins from the history (rather than mark them as deleted).
     */
    PLUGINS_WITHOUT_DESCENDANTS(node -> node.getChildren().isEmpty(), false),

    /**
     * This mode discovers only identified plugins or their descendants that have no children or
     * only children that are marked as deleted (through {@link eu.europeana.metis.core.workflow.plugins.DataStatus#DELETED}).
     * The node itself is not allowed to be deleted. Note: in case that we're removing plugins, we
     * should only use this mode if we intend to mark plugins as deleted (rather than actually
     * delete the history).
     */
    PLUGINS_WITH_ONLY_DELETED_DESCENDANTS(node -> node.getChildren().isEmpty(), true);

    private final Predicate<ExecutionPluginNode> pluginTest;
    private final boolean attemptToIgnoreDeletedPlugins;

    DiscoveryMode(Predicate<ExecutionPluginNode> pluginTest,
            boolean attemptToIgnoreDeletedPlugins) {
      this.pluginTest = pluginTest;
      this.attemptToIgnoreDeletedPlugins = attemptToIgnoreDeletedPlugins;
    }
  }

  /**
   * Constructor.
   *
   * @param morphiaDatastoreProvider Access to the database.
   * @param discoveryMode The discoveryMode in which to operate.
   */
  AbstractPluginIdentification(MorphiaDatastoreProvider morphiaDatastoreProvider,
      DiscoveryMode discoveryMode) {
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
  final List<ExecutionPluginNode> discoverPlugins() {

    // Compute all iterations
    final List<List<ExecutionPluginNode>> iterations = new ArrayList<>();
    final AtomicInteger totalNodeCount = new AtomicInteger(0);
    final AtomicInteger totalRecordCount = new AtomicInteger(0);
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

        // Get the forest for this dataset, and log if needed.
        final ExecutionPluginForest forest = getForest(datasetId, nodeIdsToSkip);
        if (iterations.isEmpty() && forest!=null){
          final List<ExecutionPluginNode> allNodes = forest.getNodes(node->true);
          totalNodeCount.addAndGet(allNodes.size());
          totalRecordCount.addAndGet(allNodes.stream().mapToInt(RECORD_COUNTER).sum());
        }

        // Obtain the plugins for this dataset, in the right order for removal.
        final List<ExecutionPluginNode> plugins = Optional.ofNullable(forest)
            .map(this::identifyPlugins).orElse(Collections.emptyList()).stream()
            .map(ExecutionPluginNode::getAllInOrderOfRemoval).flatMap(List::stream)
            .filter(discoveryMode.pluginTest).collect(Collectors.toList());

        // If we find no plugins, we mark this dataset to skip. Otherwise, add them for removal.
        if (plugins.isEmpty()) {
          datasetIdsToSkip.add(datasetId);
        } else {
          nodesToRemove.addAll(plugins);
        }
      }

      // If we have no more nodes to remove, we are done.
      if (nodesToRemove.isEmpty()) {
        break;
      }

      // Save the nodes to remove as an iteration.
      iterations.add(nodesToRemove);

      // Add all nodes to remove to the node skip list: we can ignore them in the next iteration.
      nodesToRemove.stream().map(ExecutionPluginNode::getId).forEach(nodeIdsToSkip::add);

      // Log
      LOGGER.info("Iteration {} identified.", iterations.size());
    }

    // If there is nothing to do we print a message and we are done.
    if (iterations.isEmpty()) {
      LOGGER.info("Nothing to do: no plugins/leaf nodes found.");
      return Collections.emptyList();
    }

    // Print all iterations to the log.
    LOGGER.info("Total database size: {} records in {} nodes.", totalRecordCount.get(),
        totalNodeCount.get());
    LOGGER.info("{} iterations are needed:", iterations.size());
    int totalRecords = 0;
    int totalNodes = 0;
    for (int i = 0; i < iterations.size(); i++) {
      final List<ExecutionPluginNode> nodesToRemove = iterations.get(i);
      final long linkCheckingExecutions = nodesToRemove.stream().map(ExecutionPluginNode::getType)
          .filter(type -> type == PluginType.LINK_CHECKING).count();
      final long nonExecutablePlugins = nodesToRemove.stream().map(ExecutionPluginNode::getPlugin)
          .filter(plugin -> !(plugin instanceof AbstractExecutablePlugin)).count();
      final int recordCount = nodesToRemove.stream().mapToInt(RECORD_COUNTER).sum();
      LOGGER.info(
          " ... Iteration {}: remove {} nodes ({} records). This includes {} link checking plugins and {} non-executable plugins.",
          i, nodesToRemove.size(), recordCount, linkCheckingExecutions, nonExecutablePlugins);
      totalRecords += recordCount;
      totalNodes += nodesToRemove.size();
    }
    LOGGER.info("{} records will be removed in {} nodes.", totalRecords, totalNodes);

    // Return just the first iteration.
    return iterations.get(0);
  }

  private ExecutionPluginForest getForest(String datasetId, Set<String> idsToSkip) {
    try {
      return new ExecutionPluginForest(getWorkflowExecutions(datasetId),
              discoveryMode.attemptToIgnoreDeletedPlugins, idsToSkip);
    } catch (RuntimeException e) {
      LOGGER.warn("Problem with dataset {}.", datasetId, e);
      return null;
    }
  }

  abstract List<ExecutionPluginNode> identifyPlugins(ExecutionPluginForest forest);

  private Set<String> getDatasetIds() {
    final MongoCollection<WorkflowExecution> collection = morphiaDatastoreProvider.getDatastore()
            .getMapper().getCollection(WorkflowExecution.class);
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {
      final Set<String> datasetIds = new HashSet<>();
      collection.distinct("datasetId", String.class).cursor().forEachRemaining(datasetIds::add);
      return datasetIds;
    });
  }

  private List<WorkflowExecution> getWorkflowExecutions(String datasetId) {
    return ExternalRequestUtil.retryableExternalRequestForNetworkExceptions(() -> {
      final Query<WorkflowExecution> query = morphiaDatastoreProvider.getDatastore()
              .find(WorkflowExecution.class).filter(Filters.eq("datasetId", datasetId));
      return MorphiaUtils.getListOfQueryRetryable(query);
    });
  }
}
