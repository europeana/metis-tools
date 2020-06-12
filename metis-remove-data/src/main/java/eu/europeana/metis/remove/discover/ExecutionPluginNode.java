package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * For details on the functionality of this class, see the description of {@link
 * ExecutionPluginForest}.
 */
class ExecutionPluginNode {

  private final WorkflowExecution execution;
  private final AbstractMetisPlugin plugin;
  private final List<ExecutionPluginNode> children = new ArrayList<>();
  private ExecutionPluginNode parent;
  private boolean hasNonLinkCheckingDescendant = false;

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

  final PluginType getType() {
    return this.plugin.getPluginType();
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
    if (child.hasNonLinkCheckingDescendant || child.getType() != PluginType.LINK_CHECKING) {
      registerNonLinkCheckingDescendant();
    }
  }

  private void registerNonLinkCheckingDescendant() {
    if (!hasNonLinkCheckingDescendant) {
      hasNonLinkCheckingDescendant = true;
      if (parent != null) {
        parent.registerNonLinkCheckingDescendant();
      }
    }
  }

  /**
   * @return Whether this node is a leaf node, in accordance with the definition in the description
   * of {@link ExecutionPluginForest}.
   */
  boolean isLeaf() {
    return !hasNonLinkCheckingDescendant && (parent == null || parent.hasNonLinkCheckingDescendant);
  }

  /**
   * This method finds subtrees of this tree (a node including it's children may be considered a
   * tree) that satisfy the given predicate. The subtrees (identified by their top node) are
   * returned by calling the given consumer. The subtrees don't overlap.
   *
   * @param test The predicate with which to test the subtrees.
   * @param result The consumer that accepts the results.
   */
  void findSubtrees(Predicate<ExecutionPluginNode> test, Consumer<ExecutionPluginNode> result) {
    if (test.test(this)) {
      result.accept(this);
    } else {
      children.forEach(child -> child.findSubtrees(test, result));
    }
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
    while (true) {

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

  /**
   * Checks whether the plugin was started before the given date. If the plugin was not started at
   * all, this method answers negatively.
   *
   * @param date The cut-off date.
   * @return Whether the plugin was started before the given date.
   */
  boolean wasStartedBefore(Instant date) {
    return plugin.getStartedDate() != null && date.isAfter(plugin.getStartedDate().toInstant());
  }

  /**
   * Checks whether the plugin was started or canceled before the given date. If the plugin was not
   * started at all, but cancelled preemptively because one of the predecessors failed (before the
   * given date) this method also answers positively.
   *
   * @param date The cut-off date.
   * @return Whether the plugin was started or canceled before the given date.
   */
  boolean wasStartedOrCancelledBefore(Instant date) {

    // Loop back through history.
    ExecutionPluginNode current = this;
    while (current != null) {

      // If we have a started date, we can answer immediately, also for any cancelled successors.
      if (current.getPlugin().getStartedDate() != null) {
        return date.isAfter(current.getPlugin().getStartedDate().toInstant());
      }

      // So there is no started date. If it was cancelled preemptively, check predecessors.
      if (current.getPlugin().getPluginStatus() == PluginStatus.CANCELLED) {
        current = current.getParent();
        continue;
      }

      // So if it is not started and not cancelled, something strange must be going on.
      return false;
    }

    // If we are here, we know that the whole execution was cancelled. We look
    // at the creation date of the execution (which should always be available).
    return execution.getCreatedDate() != null && date
        .isAfter(execution.getCreatedDate().toInstant());
  }
}
