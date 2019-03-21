package eu.europeana.metis.migration.results.model;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.TreeMap;

public class MigrationResults {

  private final EnumMap<PluginType, Map<String, MigrationResult>> migrationResults = new EnumMap(
      PluginType.class);

  public void add(MigrationResult migrationResult) {

    // Get or create the map for the specific pluginType.
    final Map<String, MigrationResult> mapToAddTo = migrationResults
        .computeIfAbsent(migrationResult.getPluginType(), k -> new TreeMap<>());

    // Add if not present or if an older value is present.
    mapToAddTo.merge(migrationResult.getDatasetId(), migrationResult, this::chooseNewer);
  }

  private MigrationResult chooseNewer(MigrationResult candidate1, MigrationResult candidate2) {
    // Here we assume that the run IDs are strictly increasing.
    final int comparison = candidate1.getRunId().compareTo(candidate2.getRunId());
    return comparison > 0 ? candidate1 : candidate2;
  }

  // Result will be sorted due to the use of the tree map.
  public Map<String, MigrationResult> getMigrationResults(PluginType pluginType) {
    return Collections
        .unmodifiableMap(migrationResults.getOrDefault(pluginType, Collections.emptyMap()));
  }
}
