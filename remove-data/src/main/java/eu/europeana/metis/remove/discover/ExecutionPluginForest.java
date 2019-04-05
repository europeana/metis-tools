package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class ExecutionPluginForest {

  private final Map<String, ExecutionPluginNode> nodesById = new HashMap<>();
  private final Map<PluginLinkByTypeAndTimestamp, ExecutionPluginNode> nodesByTypeAndTimestamp;
  private final List<ExecutionPluginNode> rootNodes;

  /**
   * These are non-link checking nodes with only link checking descendants.
   */
  private final Set<String> leafNodeIds;

  ExecutionPluginForest(List<WorkflowExecution> executions) {
    this(executions, Collections.emptySet());
  }

  ExecutionPluginForest(List<WorkflowExecution> executions, Set<String> ignoredPluginIds) {

    // Convert all executions and plugins to nodes and add them to the node map
    for (WorkflowExecution execution : executions) {
      for (AbstractMetisPlugin plugin : execution.getMetisPlugins()) {
        if (!ignoredPluginIds.contains(plugin.getId())) {
          final ExecutionPluginNode node = new ExecutionPluginNode(execution, plugin);
          nodesById.put(node.getId(), node);
        }
      }
    }

    // Add all nodes to the lookup map for predecessors
    final Function<ExecutionPluginNode, PluginLinkByTypeAndTimestamp> createPredecessorKey = node -> new PluginLinkByTypeAndTimestamp(
        node.getPlugin().getPluginType(), node.getPlugin().getStartedDate());
    this.nodesByTypeAndTimestamp = nodesById.values().stream()
        .filter(node -> node.getPlugin().getStartedDate() != null)
        .collect(Collectors.toMap(createPredecessorKey, Function.identity()));

    // Link parents and children (predecessors and successors)
    for (ExecutionPluginNode node : nodesById.values()) {
      final ExecutionPluginNode predecessor = findPredecessor(node);
      if (predecessor != null) {
        predecessor.addChild(node);
      }
    }

    // Compose the list of root nodes
    rootNodes = nodesById.values().stream().filter(node -> node.getParent() == null)
        .collect(Collectors.toList());

    // Find all leaf nodes (candidates for removal)
    leafNodeIds = nodesById.values().stream()
        .filter(node -> node.getPlugin().getPluginType() != PluginType.LINK_CHECKING)
        .filter(ExecutionPluginNode::hasOnlyLinkCheckingDescendants)
        .map(ExecutionPluginNode::getId).collect(Collectors.toSet());
  }

  int getNodeCount() {
    return nodesById.size();
  }

  int getTreeCount() {
    return rootNodes.size();
  }

  int getLeafCount() {
    return leafNodeIds.size();
  }

  private ExecutionPluginNode findPredecessor(ExecutionPluginNode node) {

    // Analize the previous plugin information given in the plugin metadata.
    final Date previousPluginDate = node.getPlugin().getPluginMetadata()
        .getRevisionTimestampPreviousPlugin();
    final PluginType previousPluginType = PluginType.getPluginTypeFromEnumName(
        node.getPlugin().getPluginMetadata().getRevisionNamePreviousPlugin());
    final ExecutionPluginNode previous;
    if (previousPluginDate != null && previousPluginType != null) {

      // If a predecessor is named, find it.
      previous = nodesByTypeAndTimestamp.get(new PluginLinkByTypeAndTimestamp(previousPluginType,
          previousPluginDate));
      if (previous == null) {
        final String message = String
            .format("Problem with plugin %s: cannot find predecessor with date %s and type %s.",
                node.getId(), previousPluginDate, previousPluginType);
        throw new IllegalStateException(message);
      }
    } else if (previousPluginDate != null || previousPluginType != null) {

      // If one of them is present, we have an inconsistency.
      throw new IllegalStateException("Problem with plugin " + node.getId()
          + ": either the previous plugin type or the previous plugin date is null (but not both).");
    } else {

      // Otherwise look at the workflow and get the previous one.
      final int pluginIndex = IntStream.range(0, node.getExecution().getMetisPlugins().size())
          .filter(index -> node.getExecution().getMetisPlugins().get(index).getId()
              .equals(node.getId())).findFirst().orElseThrow(IllegalStateException::new);
      if (pluginIndex > 0) {
        previous = nodesById
            .get(node.getExecution().getMetisPlugins().get(pluginIndex - 1).getId());
      } else {
        previous = null;
      }
    }

    // Check: we only expect harvesting plugins to have no ancestor.
    final boolean isHarvesting = node.getPlugin().getPluginType() == PluginType.OAIPMH_HARVEST
        || node.getPlugin().getPluginType() == PluginType.HTTP_HARVEST;
    if (isHarvesting != (previous == null)) {
      throw new IllegalStateException("Problem with plugin " + node.getId()
          + ": this is a harvesting plugin with a predecessor.");
    }

    // Done
    return previous;
  }

  private boolean isLeaf(ExecutionPluginNode node) {
    return leafNodeIds.contains(node.getId());
  }

  /**
   * This method returns all leafs (i.e. non-link checking nodes with only link checking
   * descendants) that have plugin status {@link PluginStatus#FAILED} or {@link
   * PluginStatus#CANCELLED} and started execution before the given cutoff date. These leafs (and
   * all their descendants) can be safely removed.
   *
   * @param startedBeforeDate The cutoff date.
   * @return The nodes.
   */
  List<ExecutionPluginNode> getFailedOrCancelledLeafs(Instant startedBeforeDate) {
    final Predicate<ExecutionPluginNode> failedOrCancelledCheck = node ->
        node.getPlugin().getPluginStatus() == PluginStatus.FAILED
            || node.getPlugin().getPluginStatus() == PluginStatus.CANCELLED;
    return nodesById.values().stream().filter(this::isLeaf)
        .filter(node -> node.wasStartedBefore(startedBeforeDate))
        .filter(failedOrCancelledCheck).collect(Collectors.toList());
  }

  /**
   * <p>
   * This method returns all leafs (i.e. non-link checking nodes with only link checking
   * descendants) that satisfy the following conditions:
   * <ol>
   * <li>The leaf has plugin status {@link PluginStatus#RUNNING}</li>
   * <li>The leaf started execution before the given cutoff date.</li>
   * <li>There is another execution (not necessarily a leaf) of the same type that started
   * execution later, but still before the cutoff date. This execution effectively supersedes
   * the leaf.</li>
   * </ol>
   * These leafs (and all their descendants) can be safely removed.
   * </p>
   * <p>
   * Note: this method does not find executions that are superseded by an execution that happened
   * AFTER the cutoff date.
   * </p>
   *
   * @param startedBeforeDate The cutoff date.
   * @return The nodes.
   */
  List<ExecutionPluginNode> getFinishedSupersededLeafs(Instant startedBeforeDate) {

    // Get all finished executions (not necessarily leafs) before the cutoff date, grouped by type.
    final Predicate<ExecutionPluginNode> finishedCheck = node -> node.getPlugin().getPluginStatus()
        == PluginStatus.FINISHED;
    final Map<PluginType, List<ExecutionPluginNode>> finishedPluginsByType = nodesById.values()
        .stream().filter(node -> node.wasStartedBefore(startedBeforeDate)).filter(finishedCheck)
        .collect(Collectors.groupingBy(node -> node.getPlugin().getPluginType()));

    // Find the superseded executions by removing the last execution. Finally filter for leafs.
    final Comparator<ExecutionPluginNode> reverseStartedDateComparator = Comparator.<ExecutionPluginNode, Date>comparing(
        node -> node.getPlugin().getStartedDate()).reversed();
    final Function<List<ExecutionPluginNode>, Stream<ExecutionPluginNode>> removeLatestExecution = nodes ->
        nodes.stream().sorted(reverseStartedDateComparator).skip(1);
    return finishedPluginsByType.values().stream().flatMap(removeLatestExecution)
        .filter(this::isLeaf).collect(Collectors.toList());
  }
}
