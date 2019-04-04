package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class ExecutionPluginForest {

  private final Map<String, Node> nodesById = new HashMap<>();
  private final Map<PluginLinkByTypeAndTimestamp, Node> nodesByTypeAndTimestamp;

  ExecutionPluginForest(List<WorkflowExecution> executions) {

    // Convert all executions and plugins to nodes and add them to the node map
    for (WorkflowExecution execution : executions) {
      for (AbstractMetisPlugin plugin : execution.getMetisPlugins()) {
        final Node node = new Node(execution, plugin);
        nodesById.put(node.getId(), node);
      }
    }

    // Add all nodes to the lookup map for predecessors
    final Function<Node, PluginLinkByTypeAndTimestamp> createPredecessorKey = node -> new PluginLinkByTypeAndTimestamp(
        node.getPlugin().getPluginType(), node.getPlugin().getStartedDate());
    this.nodesByTypeAndTimestamp = nodesById.values().stream()
        .filter(node -> node.getPlugin().getStartedDate() != null)
        .collect(Collectors.toMap(createPredecessorKey, Function.identity()));

    // Link parents and children (predecessors and successors)
    for (Node node : nodesById.values()) {
      final Node predecessor = findPredecessor(node);
      if (predecessor != null) {
        predecessor.addChild(node);
      }
    }
  }

  int getNodeCount() {
    return nodesById.size();
  }

  private Node findPredecessor(Node node) {

    // Analize the previous plugin information given in the plugin metadata.
    final Date previousPluginDate = node.getPlugin().getPluginMetadata()
        .getRevisionTimestampPreviousPlugin();
    final PluginType previousPluginType = PluginType.getPluginTypeFromEnumName(
        node.getPlugin().getPluginMetadata().getRevisionNamePreviousPlugin());
    final Node previous;
    if (previousPluginDate != null && previousPluginType != null) {

      // If a predecessor is named, find it.
      previous = nodesByTypeAndTimestamp.get(new PluginLinkByTypeAndTimestamp(previousPluginType,
          previousPluginDate));
      if (previous == null) {
        throw new IllegalStateException(
            "Problem with plugin " + node.getId() + ": cannot find predecessor with date "
                + previousPluginDate + " and type " + previousPluginType + ".");
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

  private static class Node {

    private final WorkflowExecution execution;
    private final AbstractMetisPlugin plugin;
    private final List<Node> children = new ArrayList<>();
    private Node parent;

    Node(WorkflowExecution execution, AbstractMetisPlugin plugin) {
      this.execution = execution;
      this.plugin = plugin;
      if (this.execution == null || this.plugin == null || getId() == null) {
        throw new IllegalArgumentException();
      }
    }

    final String getId() {
      return this.plugin.getId();
    }

    WorkflowExecution getExecution() {
      return execution;
    }

    AbstractMetisPlugin getPlugin() {
      return plugin;
    }

    Node getParent() {
      return parent;
    }

    List<Node> getChildren() {
      return Collections.unmodifiableList(children);
    }

    void addChild(Node child) {
      if (child.getParent() != null) {
        throw new IllegalStateException();
      }
      child.parent = this;
      this.children.add(child);
    }
  }
}
