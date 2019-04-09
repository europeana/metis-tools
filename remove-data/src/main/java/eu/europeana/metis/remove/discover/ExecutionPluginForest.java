package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.time.Instant;
import java.util.ArrayList;
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

/**
 * <p>
 * This class represents a collection of execution plugins. They are organized in trees based on the
 * data flow: if a plugin operates on the result of another one, this means that it is a child of
 * the other one. In this way a collection of trees is formed, one for each harvest execution (as
 * these are the only one that don't have a predecessor). The nodes in the tree are instances of
 * {@link ExecutionPluginNode}.
 * </p>
 * <p>
 * We also keep track of the 'leafs' of the tree: leafs are nodes (i.e. plugin executions) that we
 * can remove without this breaking the tree. The difference with regular leafs is that link
 * checking executions have special rules. We define a leaf to be any node (either link checking or
 * non-link checking) such that:
 * <ol>
 * <li>
 * It doesn't have descendants, or, if it does, all descendants are link checking executions.
 * </li>
 * <li>
 * It doesn't have a parent, or, if it does, the parent is not a link checking execution.
 * </li>
 * <li>
 * It doesn't have a parent, or, if it does, it or at least one of the siblings is not a link
 * checking execution.
 * </li>
 * </ol>
 * Note that this means that a leaf can be either a link checking execution (if at least one of its
 * siblings is not) or a regular plugin execution. Also, leafs are not descendants or ancestors of
 * other leafs. And, most importantly: All nodes without children are either leafs or descendants of
 * leafs (i.e. no nodes that are leafs in the traditional sense are forgotten).
 * </p>
 * <p>
 * The reason why we define leafs this way is that the criteria for removing executions should be
 * tested on regular plugin executions, not just on the link checking executions that follow them.
 * If an execution can be removed, but it is followed by a link checking, the link checking can also
 * be removed, so it is important to check the regular plugin execution against these criteria.
 * </p>
 */
class ExecutionPluginForest {

  private final Map<String, ExecutionPluginNode> nodesById = new HashMap<>();
  private final Map<PluginLinkByTypeAndTimestamp, ExecutionPluginNode> nodesByTypeAndTimestamp;
  private final List<ExecutionPluginNode> rootNodes;
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
    final Function<ExecutionPluginNode, PluginLinkByTypeAndTimestamp> extractTimeAndTimestamp = node -> new PluginLinkByTypeAndTimestamp(
        node.getType(), node.getPlugin().getStartedDate());
    this.nodesByTypeAndTimestamp = nodesById.values().stream()
        .filter(node -> node.getPlugin().getStartedDate() != null)
        .collect(Collectors.toMap(extractTimeAndTimestamp, Function.identity()));

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
    this.leafNodeIds = nodesById.values().stream().filter(ExecutionPluginNode::isLeaf)
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

      // If a predecessor is named, find it. This is for plugins that have started executing.
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

      // Otherwise look at the workflow and get the previous one. This is for plugins that have not
      // started executing (i.e. they are waiting for a previous plugin to finish or they were
      // recursively cancelled due to a previous plugin being cancelled or failing).
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
    final boolean isHarvesting =
        node.getType() == PluginType.OAIPMH_HARVEST || node.getType() == PluginType.HTTP_HARVEST;
    if (isHarvesting != (previous == null)) {
      throw new IllegalStateException("Problem with plugin " + node.getId()
          + ": this is a harvesting plugin with a predecessor.");
    }

    // Done
    return previous;
  }

  /**
   * This method returns all leafs that have plugin status {@link PluginStatus#FAILED} or {@link
   * PluginStatus#CANCELLED} and started execution before the given cutoff date. These leafs (along
   * with any descendants) can be safely removed. Note that if the leaf itself does not meet the
   * criteria, maybe one of the descendants does, and then we return that.
   *
   * @param startedBeforeDate The cutoff date.
   * @return The nodes.
   */
  List<ExecutionPluginNode> getFailedOrCancelledLeafs(Instant startedBeforeDate) {

    // Build the criteria checks: one for the state and one for the time.
    final Predicate<ExecutionPluginNode> failedOrCancelledCheck = node ->
        node.getPlugin().getPluginStatus() == PluginStatus.FAILED
            || node.getPlugin().getPluginStatus() == PluginStatus.CANCELLED;
    final Predicate<ExecutionPluginNode> startedBeforeCheck = node -> node
        .wasStartedBefore(startedBeforeDate);

    // Collect all leafs or descendants of leafs that satisfy the criteria. Optimize by checking the
    // time before even considering the leaf node: all descendants must have started later.
    final List<ExecutionPluginNode> result = new ArrayList<>();
    leafNodeIds.stream().map(nodesById::get).filter(startedBeforeCheck).forEach(
        node -> node.findSubtrees(failedOrCancelledCheck.and(startedBeforeCheck), result::add));
    return result;
  }

  /**
   * <p>
   * This method returns all leafs that satisfy the following conditions:
   * <ol>
   * <li>The leaf has plugin status {@link PluginStatus#FINISHED}</li>
   * <li>The leaf started execution before the given cutoff date.</li>
   * <li>There is another execution (not necessarily a leaf) of the same type that started
   * execution later, but still before the cutoff date. This execution effectively supersedes the
   * leaf.</li>
   * </ol>
   * These leafs (and all their descendants) can be safely removed. Note that if the leaf itself
   * does not meet the criteria, maybe one of the descendants does, and then we return that.
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
        .collect(Collectors.groupingBy(ExecutionPluginNode::getType));

    // Find the superseded executions by removing the last execution.
    final Comparator<ExecutionPluginNode> reverseStartedDateComparator = Comparator.<ExecutionPluginNode, Date>comparing(
        node -> node.getPlugin().getStartedDate()).reversed();
    final Function<List<ExecutionPluginNode>, Stream<ExecutionPluginNode>> removeLatestExecution = nodes ->
        nodes.stream().sorted(reverseStartedDateComparator).skip(1);
    final Set<String> supersededExecutions = finishedPluginsByType.values().stream()
        .flatMap(removeLatestExecution).map(ExecutionPluginNode::getId).collect(Collectors.toSet());

    // Find the leafs or descendants of leafs that are marked as superseded.
    final List<ExecutionPluginNode> result = new ArrayList<>();
    final Predicate<ExecutionPluginNode> isSuperseded = node -> supersededExecutions
        .contains(node.getId());
    leafNodeIds.stream().map(nodesById::get)
        .forEach(node -> node.findSubtrees(isSuperseded, result::add));
    return result;
  }
}
