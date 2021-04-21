package eu.europeana.metis.remove.discover;

import eu.europeana.metis.core.mongo.MorphiaDatastoreProvider;
import java.util.List;
import java.util.function.Predicate;

/**
 * Identifies all plugins that belong to a certain dataset.
 */
public class DatasetSpecificIdentification extends AbstractPluginIdentification {

  private static final String DATASET_ID_TO_DELETE = "9";

  /**
   * Constructor.
   *
   * @param morphiaDatastoreProvider Access to the database.
   */
  DatasetSpecificIdentification(MorphiaDatastoreProvider morphiaDatastoreProvider) {
    super(morphiaDatastoreProvider,  DiscoveryMode.PLUGINS_WITHOUT_DESCENDANTS);
  }

  @Override
  List<ExecutionPluginNode> identifyPlugins(ExecutionPluginForest forest) {
    final Predicate<ExecutionPluginNode> datasetIdCheck = node ->
        DATASET_ID_TO_DELETE.equals(node.getExecution().getDatasetId());
    return forest.getLeafSubtrees(datasetIdCheck);
  }
}
