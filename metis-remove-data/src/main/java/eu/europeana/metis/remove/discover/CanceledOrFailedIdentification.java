package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;

/**
 * This identification algorithm identifies nodes for plugins that failed or were canceled, and were
 * run (i.e. started) before the latest successful index to publish action for the same dataset. In
 * effect, this finds any failed or canceled plugins for data that was later successfully
 * published.
 */
public class CanceledOrFailedIdentification extends AbstractOrphanIdentification {

  /**
   * Constructor.
   *
   * @param morphiaDatastoreProvider Access to the database.
   */
  CanceledOrFailedIdentification(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    super(morphiaDatastoreProvider, DiscoveryMode.ORPHANS_WITH_ONLY_DELETED_DESCENDANTS);
  }

  @Override
  List<ExecutionPluginNode> identifyOrphans(ExecutionPluginForest forest) {

    // Find the most recent successful index to publish start date (or the min date if none available).
    // Note: we'll look at the execution start date, to make sure that our comparison below holds.
    final Predicate<ExecutionPluginNode> successfulPublishCheck = node ->
            node.getPlugin().getPluginType() == PluginType.PUBLISH
                    && node.getPlugin().getPluginStatus() == PluginStatus.FINISHED;
    final Instant latestSuccessfulPublishDate = forest.getNodes(successfulPublishCheck)
            .stream().map(ExecutionPluginNode::getExecution)
            .map(WorkflowExecution::getStartedDate).map(Date::toInstant)
            .reduce(Instant.MIN, (i1, i2) -> i1.isAfter(i2) ? i1 : i2);

    // Get the canceled and failed orphans that occurred before the latest successful index.
    // Note: we check the time of the workflow because the time of the plugin may not be set
    // if the plugin was canceled before it started.
    final Predicate<ExecutionPluginNode> isBeforeLatestPluginCheck = node ->
            node.getExecution().getStartedDate() != null &&
                    node.getExecution().getStartedDate().toInstant()
                            .isBefore(latestSuccessfulPublishDate);
    final Predicate<ExecutionPluginNode> failedOrCancelledCheck = node ->
            node.getPlugin().getPluginStatus() == PluginStatus.FAILED
                    || node.getPlugin().getPluginStatus() == PluginStatus.CANCELLED;
    return forest.getOrphanLeafSubtrees(failedOrCancelledCheck.and(isBeforeLatestPluginCheck));
  }
}
