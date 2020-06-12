package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * <b>This orphan identification is for cleaning up the migration results of the Metis migration in
 * 2018. Given an instance of {@link ExecutionPluginForest}, it finds two kinds of nodes that can be
 * safely removed:</b>
 * <ol>
 * <li>
 * All leafs or descendants of leafs that have plugin status {@link PluginStatus#FAILED} or {@link
 * PluginStatus#CANCELLED} and started execution before the given cutoff date.
 * </li>
 * <li>
 * All leafs or descendants of leafs that have been superseded. This means that the leaf
 * (descendant) satisfies the following conditions:
 * <ol style="list-style-type: lower-alpha">
 * <li>The leaf has plugin status {@link PluginStatus#FINISHED}</li>
 * <li>The leaf started execution before the given cutoff date.</li>
 * <li>There is another execution (not necessarily a leaf) of the same type that started
 * execution later, but still before the cutoff date, and also has plugin status {@link
 * PluginStatus#FINISHED}. This execution effectively supersedes the leaf.</li>
 * </ol>
 * Note: this method does not find executions that are superseded by an execution that happened
 * AFTER the cutoff date.
 * </li>
 * </ol>
 */
class MigrationCleanupIdentification extends AbstractOrphanIdentification {

  /**
   * Create the cutoff date. This date is chosen so that one or two hours of difference (due to time
   * zones) does not matter. However, this should be fine as the execution objects contain time
   * zone-independent Date objects.
   **/
  private static final Instant END_OF_MIGRATION = LocalDateTime.of(2018, Month.OCTOBER, 13, 0, 0)
      .atZone(ZoneId.systemDefault()).toInstant();

  /**
   * Constructor.
   *
   * @param morphiaDatastoreProvider Access to the database.
   * @param discoveryMode The discoveryMode in which to operate.
   */
  MigrationCleanupIdentification(MorphiaDatastoreProvider morphiaDatastoreProvider, DiscoveryMode discoveryMode) {
    super(morphiaDatastoreProvider, discoveryMode);
  }

  @Override
  List<ExecutionPluginNode> identifyOrphans(ExecutionPluginForest forest) {

    // Add the two orphan types to the same list: the sets are disjoint so we won't have duplicates.
    final List<ExecutionPluginNode> result = new ArrayList<>();
    result.addAll(getFailedOrCancelledLeafs(forest));
    result.addAll(getFinishedSupersededLeafs(forest));
    return result;
  }

  private static List<ExecutionPluginNode> getFailedOrCancelledLeafs(ExecutionPluginForest forest) {

    // Build the criteria checks: one for the state and one for the time.
    final Predicate<ExecutionPluginNode> failedOrCancelledCheck = node ->
        node.getPlugin().getPluginStatus() == PluginStatus.FAILED
            || node.getPlugin().getPluginStatus() == PluginStatus.CANCELLED;
    final Predicate<ExecutionPluginNode> startedOrCancelledBeforeCheck = node -> node
        .wasStartedOrCancelledBefore(END_OF_MIGRATION);

    // Collect all leafs or descendants of leafs that satisfy the criteria. Optimize by checking the
    // time before even considering the leaf node: all descendants must have started later.
    return forest.getOrphanLeafSubtrees(startedOrCancelledBeforeCheck,
        failedOrCancelledCheck.and(startedOrCancelledBeforeCheck));
  }

  private static List<ExecutionPluginNode> getFinishedSupersededLeafs(
      ExecutionPluginForest forest) {

    // Get all finished executions (not necessarily leafs) before the cutoff date, grouped by type.
    final Predicate<ExecutionPluginNode> startedBeforeCheck = node -> node
        .wasStartedBefore(END_OF_MIGRATION);
    final Predicate<ExecutionPluginNode> finishedCheck = node -> node.getPlugin().getPluginStatus()
        == PluginStatus.FINISHED;
    final Map<PluginType, List<ExecutionPluginNode>> finishedPluginsByType = forest.getNodes(
        startedBeforeCheck.and(finishedCheck)).stream()
        .collect(Collectors.groupingBy(ExecutionPluginNode::getType));

    // Find the superseded executions by removing the last execution.
    final Comparator<ExecutionPluginNode> reverseStartedDateComparator = Comparator.<ExecutionPluginNode, Date>comparing(
        node -> node.getPlugin().getStartedDate()).reversed();
    final Function<List<ExecutionPluginNode>, Stream<ExecutionPluginNode>> removeLatestExecution = nodes ->
        nodes.stream().sorted(reverseStartedDateComparator).skip(1);
    final Set<String> supersededExecutions = finishedPluginsByType.values().stream()
        .flatMap(removeLatestExecution).map(ExecutionPluginNode::getId).collect(Collectors.toSet());

    // Find the leafs or descendants of leafs that are marked as superseded.
    final Predicate<ExecutionPluginNode> isSuperseded = node -> supersededExecutions
        .contains(node.getId());
    return forest.getOrphanLeafSubtrees(isSuperseded);
  }
}
