package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * This orphan identification is for cleaning up the migration results of the Metis migration in
 * 2018.
 */
public class MigrationCleanupIdentification extends AbstractOrphanIdentification {

  MigrationCleanupIdentification(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    super(morphiaDatastoreProvider);
  }

  @Override
  List<ExecutionPluginNode> identifyOrphans(ExecutionPluginForest forest) {

    // Create the cutoff date. This date is chosen so that one or two hours of difference (due to
    // time zones) does not matter. However, this should be fine as the execution objects contain
    // time zone-independent Date objects.
    final Instant cutoffDate = LocalDateTime.of(2018, Month.OCTOBER, 13, 0, 0)
        .atZone(ZoneId.systemDefault()).toInstant();

    // Analyze the forest
    final List<ExecutionPluginNode> result = new ArrayList<>();
    result.addAll(forest.getFailedOrCancelledLeafs(cutoffDate));
    result.addAll(forest.getFinishedSupersededLeafs(cutoffDate));
    return result;
  }
}
