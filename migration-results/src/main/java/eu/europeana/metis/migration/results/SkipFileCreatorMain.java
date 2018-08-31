package eu.europeana.metis.migration.results;

import eu.europeana.metis.migration.results.model.MigrationResult;
import eu.europeana.metis.migration.results.model.MigrationResult.ResultStatus;
import eu.europeana.metis.migration.results.model.MigrationResults;
import eu.europeana.metis.migration.results.model.PluginType;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This file creates the skip files and also does the full analysis on everything that has
 * happened.
 */
public class SkipFileCreatorMain {

  private static final String HARVEST_SKIP_FILE = "/home/jochen/migration/new_skip_lists/processed-datasets-harvesting.log";
  private static final String PREVIEW_SKIP_FILE = "/home/jochen/migration/new_skip_lists/processed-datasets-preview.log";
  private static final String PUBLISH_SKIP_FILE = "/home/jochen/migration/new_skip_lists/processed-datasets-publish.log";

  private static List<String> DATASETS_TO_BE_IGNORED = Arrays
      .asList("2058621", "9200365", "2022608", "09404", "09407", "2048407", "9200386", "01004",
          "2022621", "09407b", "11621", "15402", "2022701d", "2022609", "09414h", "08501",
          "09435", "92096", "9200479", "03506", "9200384", "2048099", "2048087",
          "09407d", "92034", "2058618", "11614", "03915", "11648", "11650", "11651",
          "11654", "11649", "11652", "11656", "08504", "2020704", "2020702", "9200302", "2048212",
          "08555", "2021010", "9200401", "03931", "9200520", "2048008", "11616", "11655", "03912");
  // Datasets 11620, 9200359 are also large.

  public static void main(String[] args) throws IOException {

    // Collect the migration results4
    // NOTE: for skip files, must load ALL migration model!
    final MigrationResults migrationResults = MigrationResultParser
        .parse(MigrationResultParser.PRE_HARVEST_RUN_ID,
            MigrationResultParser.FUTURE_RUN_ID);

    // Process the model to obtain statistics
    processResultsForPluginType(migrationResults, PluginType.OAIPMH_HARVEST);
    processResultsForPluginType(migrationResults, PluginType.PREVIEW);
    processResultsForPluginType(migrationResults, PluginType.PUBLISH);

    // Create skip files
    System.out.println();
    createSkipFile(migrationResults, PluginType.OAIPMH_HARVEST, HARVEST_SKIP_FILE);
    createSkipFile(migrationResults, PluginType.PREVIEW, PREVIEW_SKIP_FILE);
    createSkipFile(migrationResults, PluginType.PUBLISH, PUBLISH_SKIP_FILE);
  }

  private static boolean resultInvalidated(MigrationResult result,
      Map<String, MigrationResult> resultsPreviousPlugin) {
    // Determines if the result is invalidated by a later result in the previous plugin: i.e. is
    // there a later result in the previous plugin that is ready for this plugin to build on.
    final MigrationResult previousPluginResult = resultsPreviousPlugin.get(result.getDatasetId());
    return previousPluginResult != null && previousPluginResult.readyForNextPlugin()
        && previousPluginResult.getRunId().compareTo(result.getRunId()) > 0;
  }

  private static void createSkipFile(MigrationResults allResults,
      PluginType pluginType, String filePath) {

    // The skip list: add the consant list of datasets to be ignored.
    final Set<String> skip = new HashSet<>(DATASETS_TO_BE_IGNORED);

    // Skip all datasets from this plugin that we should not do again.
    final Map<String, MigrationResult> resultsThisPlugin = allResults
        .getMigrationResults(pluginType);
    final Set<String> completedSetsThisPlugin = resultsThisPlugin.values().stream()
        .filter(result -> !result.shouldDoAgain()).map(MigrationResult::getDatasetId)
        .collect(Collectors.toSet());
    skip.addAll(completedSetsThisPlugin);

    // In case of preview
    if (PluginType.PREVIEW == pluginType) {
      final Map<String, MigrationResult> harvestResults = allResults
          .getMigrationResults(PluginType.OAIPMH_HARVEST);

      // First remove those datasets that have a harvest after an index to preview.
      final Set<String> invalidated = resultsThisPlugin.values().stream()
          .filter(result -> resultInvalidated(result, harvestResults))
          .map(MigrationResult::getDatasetId).collect(Collectors.toSet());
      skip.removeAll(invalidated);

      // Add datasets of harvest to the preview skip list if they are not ready for preview.
      final Set<String> failedSetsHarvest = harvestResults.values().stream()
          .filter(result -> !result.readyForNextPlugin()).map(MigrationResult::getDatasetId)
          .collect(Collectors.toSet());
      skip.addAll(failedSetsHarvest);

      // Count harvests that are ready for preview and that are not to be skipped.
      final List<MigrationResult> available = harvestResults.values().stream()
          .filter(MigrationResult::readyForNextPlugin)
          .filter(result -> !skip.contains(result.getDatasetId())).collect(Collectors.toList());
      final int recordCount = available.stream().map(MigrationResult::getProcessedRecords)
          .reduce(0, Integer::sum);
      System.out.println(String
          .format("Number of available datasets for plugin %s: %d (%d records)", pluginType,
              available.size(), recordCount));
      if (!invalidated.isEmpty()) {
        System.out.println(String
            .format("  (Of these, %d are invalidated by a later result in the previous plugin)",
                invalidated.size()));
      }
    }

    // In case of publish
    if (PluginType.PUBLISH == pluginType) {
      final Map<String, MigrationResult> harvestResults = allResults
          .getMigrationResults(PluginType.OAIPMH_HARVEST);
      final Map<String, MigrationResult> previewResults = allResults
          .getMigrationResults(PluginType.PREVIEW);

      // First remove those datasets that have an index to preview after an index to publish.
      final Set<String> invalidated = resultsThisPlugin.values().stream()
          .filter(result -> resultInvalidated(result, previewResults))
          .map(MigrationResult::getDatasetId).collect(Collectors.toSet());
      skip.removeAll(invalidated);

      // Add datasets of harvest to the publish skip list if they are not ready for preview.
      final Set<String> failedSetsHarvest = harvestResults.values().stream()
          .filter(result -> !result.readyForNextPlugin()).map(MigrationResult::getDatasetId)
          .collect(Collectors.toSet());
      skip.addAll(failedSetsHarvest);

      // Add datasets of preview to the publish skip list if they are not ready for publish.
      final Set<String> failedSetsPreview = previewResults.values().stream()
          .filter(result -> !result.readyForNextPlugin()).map(MigrationResult::getDatasetId)
          .collect(Collectors.toSet());
      skip.addAll(failedSetsPreview);

      // Add all datasets of harvest that have not been indexed for preview to skip list
      final Set<String> nonPreviewedSetsHarvest = harvestResults.values().stream()
          .map(MigrationResult::getDatasetId)
          .filter(id -> !previewResults.containsKey(id) || failedSetsPreview.contains(id))
          .collect(Collectors.toSet());
      skip.addAll(nonPreviewedSetsHarvest);

      // Count index to preview datasets that are ready for publish and that are not to be skipped.
      final List<MigrationResult> available = previewResults.values().stream()
          .filter(MigrationResult::readyForNextPlugin)
          .filter(result -> !skip.contains(result.getDatasetId())).collect(Collectors.toList());
      final int recordCount = available.stream().map(MigrationResult::getProcessedRecords)
          .reduce(0, Integer::sum);
      System.out.println(String
          .format("Number of available datasets for plugin %s: %d (%d records)", pluginType,
              available.size(), recordCount));
      if (!invalidated.isEmpty()) {
        System.out.println(String
            .format("  (Of these, %d are invalidated by a later result in the previous plugin)",
                invalidated.size()));
      }
    }

    // Create the skip files
    Path path = Paths.get(filePath);
    try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
      for (String line : skip) {
        writer.write(line);
        writer.newLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void processResultsForPluginType(MigrationResults allResults,
      PluginType pluginType) {

    // Start section: determine how many were processed.
    System.out.println(String.format("\nStatistics for plugin type %s: ", pluginType));
    final Map<String, MigrationResult> results = allResults.getMigrationResults(pluginType);
    System.out.println(String.format("  Total number of datasets processed: %d. ", results.size()));

    // How many tasks were empty
    final List<MigrationResult> resultsWereEmpty = results.values().stream()
        .filter(ResultStatus.EMPTY).collect(Collectors.toList());
    System.out.println(String.format("  Number of empty datasets: %d.", resultsWereEmpty.size()));
    if (pluginType != PluginType.OAIPMH_HARVEST) {
      resultsWereEmpty.stream().map(MigrationResult::toSummaryWithStatus)
          .forEach(System.out::println);
    }

    // How many non-empty tasks did not end normally
    final List<MigrationResult> resutsNotEndedNormally = results.values().stream()
        .filter(ResultStatus.DID_NOT_END_NORMALLY).collect(Collectors.toList());
    System.out.println(String.format("  Number of datasets that did not end normally: %d.",
        resutsNotEndedNormally.size()));
    resutsNotEndedNormally.stream().map(MigrationResult::toSummaryWithStatus)
        .forEach(System.out::println);

    // How many non-empty tasks ended normally but had errors
    final List<MigrationResult> resutsWithErrors = results.values().stream()
        .filter(ResultStatus.COMPLETED_WITH_ERRORS).collect(Collectors.toList());
    System.out
        .println(String.format("  Number of datasets that ended normally, but had errors: %d.",
            resutsWithErrors.size()));
    resutsWithErrors.stream().map(MigrationResult::toSummaryWithCounts)
        .forEach(System.out::println);

    // How many non-empty tasks ended normally, without errors, but the totals don't match.
    final List<MigrationResult> resultsWithNonMatchingTotals = results.values().stream()
        .filter(ResultStatus.TOTALS_DONT_MATCH).collect(Collectors.toList());
    System.out.println(String.format(
        "  Number of datasets that ended normally and without errors, but of which the totals don't match: %d.",
        resultsWithNonMatchingTotals.size()));
    resultsWithNonMatchingTotals.stream().map(MigrationResult::toSummaryWithCounts)
        .forEach(System.out::println);

    // How many non-empty tasks ended normally, without errors and with matching totals.
    final List<MigrationResult> successfulDatasets = results.values().stream()
        .filter(ResultStatus.SUCCESS).collect(Collectors.toList());
    final int successfulRecordCount = successfulDatasets.stream()
        .map(MigrationResult::getProcessedRecords).reduce(0, Integer::sum);
    System.out.println(String
        .format("  Number of datasets that ended successfully: %d (%d records).",
            successfulDatasets.size(), successfulRecordCount));
  }
}
