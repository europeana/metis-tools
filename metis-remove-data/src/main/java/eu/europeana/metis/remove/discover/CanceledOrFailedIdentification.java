package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import eu.europeana.metis.core.workflow.plugins.PluginStatus;
import java.util.List;
import java.util.function.Predicate;

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
    final Predicate<ExecutionPluginNode> failedOrCancelledCheck = node ->
        node.getPlugin().getPluginStatus() == PluginStatus.FAILED
            || node.getPlugin().getPluginStatus() == PluginStatus.CANCELLED;
    return forest.getOrphanLeafSubtrees(failedOrCancelledCheck);
  }
}
