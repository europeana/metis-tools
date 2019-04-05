package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.function.Predicate;

class ExecutionPluginNode {

  private final WorkflowExecution execution;
  private final AbstractMetisPlugin plugin;
  private final List<ExecutionPluginNode> children = new ArrayList<>();
  private ExecutionPluginNode parent;

  ExecutionPluginNode(WorkflowExecution execution, AbstractMetisPlugin plugin) {
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

  ExecutionPluginNode getParent() {
    return parent;
  }

  List<ExecutionPluginNode> getChildren() {
    return Collections.unmodifiableList(children);
  }

  void addChild(ExecutionPluginNode child) {
    if (child.getParent() != null) {
      throw new IllegalStateException();
    }
    child.parent = this;
    this.children.add(child);
  }

  boolean hasOnlyLinkCheckingDescendants() {
    final Predicate<ExecutionPluginNode> childTest = child ->
        child.getPlugin().getPluginType() == PluginType.LINK_CHECKING
            && child.hasOnlyLinkCheckingDescendants();
    return children.stream().allMatch(childTest);
  }

  /**
   * This method returns the children of this node (including the node itself) in order of removal.
   * This means that children need to be removed before their the parent is removed. This is
   * computed by doing a breadth-first traversal of the tree of children and returning them in
   * reverse order (deepest nodes first).
   *
   * @return The list of this node and all its descendants in order of deepest descendant first.
   */
  List<ExecutionPluginNode> getAllInOrderOfRemoval() {

    // Quick win: if there are no children, we are done quickly.
    if (children.isEmpty()) {
      return Collections.singletonList(this);
    }

    // The queue of nodes to process: the next node to process is always the one at the front.
    final Deque<ExecutionPluginNode> toProcess = new ArrayDeque<>();

    // The result list. Starts empty. Always add at the front so that we get reverse order.
    final Deque<ExecutionPluginNode> result = new ArrayDeque<>();

    // Process all the nodes. Start with this one.
    toProcess.add(this);
    while(true) {

      // Get the node: take from the front (remove from the queue).
      final ExecutionPluginNode node = toProcess.pollFirst();
      if (node == null) {
        break;
      }

      // Add node to result at the front (so that it comes before nodes higher in the tree).
      result.addFirst(node);

      // Queue children to be processed. Add at the end so they come after nodes higher in the tree.
      node.children.forEach(toProcess::addLast);
    }

    // Done
    return new ArrayList<>(result);
  }

  boolean wasStartedBefore(Instant date) {
    return plugin.getStartedDate() != null && date.isAfter(plugin.getStartedDate().toInstant());
  }
}
