package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.dao.DataEvolutionUtils;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPluginMetadata;
import eu.europeana.metis.core.workflow.plugins.DataStatus;
import eu.europeana.metis.core.workflow.plugins.ExecutablePlugin;
import eu.europeana.metis.core.workflow.plugins.ExecutablePluginType;
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
 * It doesn't have descendants, or, if it does, all descendants are link checking executions. We
 * allow link checking descendants because link checking plugins function more as add-ons to other
 * plugins and we wish to avoid them somehow blocking leaf analysis and identification.
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
  private final List<ExecutionPluginNode> rootNodes;
  private final Set<String> leafNodeIds;

  ExecutionPluginForest(List<WorkflowExecution> executions, boolean attemptToIgnoreDeletedPlugins) {
    this(executions, attemptToIgnoreDeletedPlugins, Collections.emptySet());
  }

  ExecutionPluginForest(List<WorkflowExecution> executions, boolean attemptToIgnoreDeletedPlugins,
          Set<String> ignoredPluginIds) {

    // Convert all executions and plugins to nodes and add them to the node map, depending on
    // whether they are to be ignored or not. No nodes are linked at this point.
    final  Map<String, ExecutionPluginNode> ignoredNodesById = new HashMap<>();
    for (WorkflowExecution execution : executions) {
      for (AbstractMetisPlugin plugin : execution.getMetisPlugins()) {
        final boolean ignoreNode = ignoredPluginIds.contains(plugin.getId())
                || (attemptToIgnoreDeletedPlugins && plugin.getDataStatus() == DataStatus.DELETED);
        final ExecutionPluginNode node = new ExecutionPluginNode(execution, plugin);
        (ignoreNode ? ignoredNodesById : nodesById).put(node.getId(), node);
      }
    }

    // Add all nodes to the lookup map for predecessors (also ignored ones: they could still have
    // children that are not to be ignored).
    final Function<ExecutionPluginNode, PluginLinkByTypeAndTimestamp> extractTimeAndTimestamp =
            node -> new PluginLinkByTypeAndTimestamp(node.getType(),
                    node.getPlugin().getStartedDate());
    final Map<PluginLinkByTypeAndTimestamp, ExecutionPluginNode> predecessorLookup = Stream
            .concat(nodesById.values().stream(), ignoredNodesById.values().stream())
            .filter(node -> node.getPlugin().getStartedDate() != null)
            .collect(Collectors.toMap(extractTimeAndTimestamp, Function.identity()));

    // Link parents and their children (predecessors and successors). This happens in iterations
    // because during linking we may find nodes that we planned to ignore but still have children
    // that we don't want to ignore. We loop until we find no such nodes anymore.
    final Function<String, ExecutionPluginNode> nodeByIdResolver = id -> Optional
            .ofNullable(nodesById.get(id)).orElseGet(() -> ignoredNodesById.get(id));
    Map<String, ExecutionPluginNode> nodesToProcess = nodesById;
    while (!nodesToProcess.isEmpty()) {

      // We will need a list of nodes that we planned to ignore but need to keep after all.
      final Map<String, ExecutionPluginNode> ignoredNodesToBeKept = new HashMap<>();

      // We go by all nodes in this iteration and attempt to find the parent.
      for (ExecutionPluginNode node : nodesToProcess.values()) {
        final ExecutionPluginNode predecessor = findPredecessor(node, nodeByIdResolver,
                predecessorLookup::get);
        if (predecessor != null) {

          // So we have a parent. If we don't have it in our node map yet, we mark it for
          // processing in the next iteration. We don't add it directly to the node map as we may be
          // iterating over that very collection (during the first iteration).
          if (!nodesById.containsKey(predecessor.getId())) {
            ignoredNodesToBeKept.putIfAbsent(predecessor.getId(), predecessor);
          }

          // We now add the predecessor as parent for the node.
          predecessor.addChild(node);
        }
      }

      // We add all nodes that we need to keep to our master node map. We also put them up for
      // processing during the next iteration.
      nodesById.putAll(ignoredNodesToBeKept);
      nodesToProcess = ignoredNodesToBeKept;
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

  private static ExecutionPluginNode findPredecessor(ExecutionPluginNode node,
          Function<String, ExecutionPluginNode> nodeByIdResolver,
          Function<PluginLinkByTypeAndTimestamp, ExecutionPluginNode> nodeByTypeAndTimeResolver) {

    // Analyze the previous plugin information given in the plugin metadata.
    final Date previousPluginDate = Optional.ofNullable(node.getPlugin().getPluginMetadata())
        .map(AbstractMetisPluginMetadata::getRevisionTimestampPreviousPlugin).orElse(null);
    final PluginType previousPluginType = Optional.ofNullable(node.getPlugin().getPluginMetadata())
        .map(AbstractMetisPluginMetadata::getRevisionNamePreviousPlugin)
        .map(PluginType::getPluginTypeFromEnumName).orElse(null);
    final ExecutionPluginNode previous;
    if (previousPluginDate != null && previousPluginType != null) {

      // If a predecessor is named, find it. This is for plugins that have started executing.
      previous = nodeByTypeAndTimeResolver
              .apply(new PluginLinkByTypeAndTimestamp(previousPluginType, previousPluginDate));
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
      previous = computePredecessorInWorkflow(node, nodeByIdResolver);
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

  private static ExecutionPluginNode computePredecessorInWorkflow(ExecutionPluginNode node,
          Function<String, ExecutionPluginNode> nodeByIdResolver) {

    // Determine the type of the previous plugin.
    final Set<PluginType> predecessorTypes;
    if (node.getPlugin() instanceof ExecutablePlugin) {
      // Find the latest one of the right type before the plugin we're processing.
      final ExecutablePluginType pluginType = ((ExecutablePlugin<?>) node.getPlugin())
              .getPluginMetadata().getExecutablePluginType();
      predecessorTypes = DataEvolutionUtils.getPredecessorTypes(pluginType).stream()
              .map(ExecutablePluginType::toPluginType).collect(Collectors.toSet());
    } else {
      // Just find the one immediately before the node plugin we're processing.
      predecessorTypes = Set.of(PluginType.values());
    }

    // Loop through the workflow to get the right predecessor plugin.
    AbstractMetisPlugin previousPlugin = null;
    for (AbstractMetisPlugin plugin : node.getExecution().getMetisPlugins()) {
      if (plugin.getId().equals(node.getId())) {
        // We have reached the plugin we're looking for.
        break;
      }
      if (predecessorTypes.contains(plugin.getPluginType())) {
        // We have found a candidate.
        previousPlugin = plugin;
      }
    }

    // Done.
    return previousPlugin == null ? null : nodeByIdResolver.apply(previousPlugin.getId());
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
  List<ExecutionPluginNode> getLeafSubtrees(Predicate<ExecutionPluginNode> subtreeTest) {
    return getLeafSubtrees(node -> true, subtreeTest);
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
  List<ExecutionPluginNode> getLeafSubtrees(Predicate<ExecutionPluginNode> leafTest,
      Predicate<ExecutionPluginNode> subtreeTest) {
    final List<ExecutionPluginNode> result = new ArrayList<>();
    leafNodeIds.stream().map(nodesById::get).filter(leafTest)
        .forEach(node -> node.findSubtrees(subtreeTest, result::add));
    return result;
  }
}
