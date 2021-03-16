package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
 * It doesn't have a parent, or, if it does, the parent <b>does</b> have at least one descendant
 * that is a regular plugin execution (possibly this node itself). I.e. the parent or any of its
 * ancestors do not qualify as leaf under the first condition.
 * </li>
 * </ol>
 * Note that this means that a leaf can be either a regular plugin execution or a link checking
 * execution (e.g. if one of its siblings is a regular plugin execution). Also, leafs are not
 * descendants or ancestors of other leafs. And, most importantly: All nodes without children are
 * either leafs or descendants of leafs (i.e. no nodes that are leafs in the traditional sense are
 * forgotten).
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

    // Analyze the previous plugin information given in the plugin metadata.
    final Date previousPluginDate = Optional.ofNullable(node.getPlugin().getPluginMetadata())
        .map(AbstractMetisPluginMetadata::getRevisionTimestampPreviousPlugin).orElse(null);
    final PluginType previousPluginType = Optional.ofNullable(node.getPlugin().getPluginMetadata())
        .map(AbstractMetisPluginMetadata::getRevisionNamePreviousPlugin)
        .map(PluginType::getPluginTypeFromEnumName).orElse(null);
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

    // Check: we only expect harvesting, depublishing and reindexing plugins to have no ancestor.
    final boolean isHarvesting =
        node.getType() == PluginType.OAIPMH_HARVEST || node.getType() == PluginType.HTTP_HARVEST;
    final boolean isReindexing = node.getType() == PluginType.REINDEX_TO_PREVIEW
        || node.getType() == PluginType.REINDEX_TO_PUBLISH;
    final boolean isDepublishing = node.getType() == PluginType.DEPUBLISH;
    if (isHarvesting && previous != null) {
      throw new IllegalStateException("Problem with plugin " + node.getId()
          + ": this is a harvesting plugin with a predecessor.");
    }
    if (!isHarvesting && !isReindexing && !isDepublishing && previous == null) {
      throw new IllegalStateException("Problem with plugin " + node.getId()
          + ": this plugin requires a predecessor but none was found.");
    }

    // Done
    return previous;
  }

  /**
   * This method returns all nodes (leafs or otherwise) that satisfy the given test.
   * @param test The test that the nodes need to satisfy.
   * @return The list of nodes that satisfy the given test.
   */
  List<ExecutionPluginNode> getNodes(Predicate<ExecutionPluginNode> test) {
    return nodesById.values().stream().filter(test).collect(Collectors.toList());
  }

  /**
   * This method returns all non-overlapping subtrees of leafs (identified by top node and
   * potentially including leafs themselves) that satisfy the given test. So this method will only
   * look at nodes that are leafs or descendants of leafs.
   *
   * @param subtreeTest The test that the subtrees of the leaf (including the leaf itself) need to
   * satisfy (see {@link ExecutionPluginNode#findSubtrees(Predicate, Consumer)}).
   * @return The list of non-overlapping subtrees of leafs that satisfy the given test.
   */
  List<ExecutionPluginNode> getOrphanLeafSubtrees(Predicate<ExecutionPluginNode> subtreeTest) {
    return getOrphanLeafSubtrees(node -> true, subtreeTest);
  }

  /**
   * This method returns all non-overlapping subtrees of leafs (identified by top node and
   * potentially including leafs themselves) that satisfy the given tests. So this method will only
   * * look at nodes that are leafs or descendants of leafs.
   *
   * @param leafTest The test that the leaf itself needs to satisfy.
   * @param subtreeTest The test that the subtrees of the leaf (including the leaf itself) need to
   * satisfy (see {@link ExecutionPluginNode#findSubtrees(Predicate, Consumer)}).
   * @return The list of non-overlapping subtrees of leafs that satisfy the given tests.
   */
  List<ExecutionPluginNode> getOrphanLeafSubtrees(Predicate<ExecutionPluginNode> leafTest,
      Predicate<ExecutionPluginNode> subtreeTest) {
    final List<ExecutionPluginNode> result = new ArrayList<>();
    leafNodeIds.stream().map(nodesById::get).filter(leafTest)
        .forEach(node -> node.findSubtrees(subtreeTest, result::add));
    return result;
  }
}
