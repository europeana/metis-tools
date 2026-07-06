package eu.europeana.metis.published.records.harvest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.metis.core.common.DaoFieldNames;
import eu.europeana.metis.core.dao.WorkflowExecutionDao;
import eu.europeana.metis.core.dao.WorkflowExecutionDao.ResultList;
import eu.europeana.metis.core.workflow.WorkflowExecution;
import eu.europeana.metis.core.workflow.WorkflowStatus;
import eu.europeana.metis.core.workflow.plugins.AbstractMetisPlugin;
import eu.europeana.metis.core.workflow.plugins.IndexToPublishPlugin;
import eu.europeana.metis.core.workflow.plugins.MetisPlugin;
import eu.europeana.metis.core.workflow.plugins.PluginType;
import eu.europeana.metis.published.records.harvest.utils.Application;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static final String INPUT_FILE = "/home/jochen/Desktop/published_record_harvest/sample_collection_test_upgrade.txt";

  private static final String DESTINATION_DIRECTORY = "/home/jochen/Desktop/published_record_harvest/records";

  public static void main(String[] args) throws Exception {
    try (final Application application = Application.initialize()) {
      final Map<String, Set<String>> records = readInput();
      final Map<String, List<Revision>> revisions = getLatestPublishRevisions(records.keySet(),
          application);
      downloadRecords(records, revisions, application);
    }
  }

  /**
   * @return All record IDs, separated by dataset ID.
   * @throws IOException IO issues.
   */
  private static Map<String, Set<String>> readInput() throws IOException {
    final Map<String, Set<String>> result = new HashMap<>();
    final AtomicInteger counter = new AtomicInteger();
    final AtomicInteger alreadyExistCounter = new AtomicInteger();
    LOGGER.info("Reading record IDs ...");
    try (final InputStream input = Files.newInputStream(Path.of(INPUT_FILE));
        final BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
      while (true) {
        final String line = reader.readLine();
        if (line == null) {
          break;
        }
        if (line.isBlank()) {
          continue;
        }
        final String trimmedLine = line.trim();
        final String datasetId = getDatasetId(trimmedLine);
        if (datasetId == null) {
          LOGGER.warn("Ignoring invalid input: {}", trimmedLine);
          continue;
        }
        if (Files.exists(getFile(datasetId, trimmedLine))) {
          alreadyExistCounter.incrementAndGet();
          continue;
        }
        result.computeIfAbsent(datasetId, key -> new HashSet<>()).add(trimmedLine);
        if (counter.incrementAndGet() % 1000 == 0) {
          LOGGER.info("  {} record IDs read.", counter.get());
        }
      }
    }
    LOGGER.info("Total of {} valid record IDs found.", counter.get());
    LOGGER.info("{} records have been harvested and will be skipped.", alreadyExistCounter.get());
    return result;
  }

  /**
   * @param datasets    The datasets for which to get the revisions.
   * @param application This application.
   * @return For each dataset, the latest revisions (in reverse order). This is the set of index to
   * publish revisions since (and including) that latest full processing publish.
   */
  private static Map<String, List<Revision>> getLatestPublishRevisions(Set<String> datasets,
      Application application) {
    final Map<String, List<Revision>> result = new HashMap<>();
    final WorkflowExecutionDao workflowExecutionDao = new WorkflowExecutionDao(
        application.getDatastoreProvider());
    final AtomicInteger counter = new AtomicInteger();
    LOGGER.info("Reading revisions for datasets ...");
    for (final String dataset : datasets) {
      result.put(dataset, getLatestPublishRevisions(dataset, workflowExecutionDao, application));
      if (counter.incrementAndGet() % 100 == 0) {
        LOGGER.info("  {} datasets done.", counter.get());
      }
    }
    LOGGER.info("Revisions read for a total of {} datasets.", counter.get());
    return result;
  }

  /**
   * @param dataset              The dataset for which to get the revisions.
   * @param workflowExecutionDao The DAO to use.
   * @param application          This application.
   * @return For the dataset, the latest revisions (in reverse order). This is the set of index to
   * publish revisions since (and including) that latest full processing publish.
   */
  private static List<Revision> getLatestPublishRevisions(String dataset,
      WorkflowExecutionDao workflowExecutionDao, Application application) {
    final ResultList<WorkflowExecution> reverseOrderedExecutions = workflowExecutionDao
        .getAllWorkflowExecutions(Set.of(dataset), Set.of(WorkflowStatus.FINISHED),
            DaoFieldNames.CREATED_DATE, false, 0, null, true);
    final List<Revision> result = new ArrayList<>();
    for (WorkflowExecution workflowExecution : reverseOrderedExecutions.results()) {
      for (AbstractMetisPlugin<?> plugin : workflowExecution.getMetisPlugins().reversed()) {
        if (plugin.getPluginType() != PluginType.PUBLISH
            || !(plugin instanceof IndexToPublishPlugin publishPlugin)) {
          continue;
        }
        result.add(new Revision(plugin.getPluginType().name(),
            application.getProperties().ecloudProvider, plugin.getStartedDate(), false));
        if (!publishPlugin.getPluginMetadata().isIncrementalIndexing()) {
          return result;
        }
      }
    }
    LOGGER.error("No (full processing) plugin is found for dataset {}.", dataset);
    return result;
  }

  /**
   * Download all records and save them to the right place.
   *
   * @param records     The records sorted by dataset.
   * @param revisions   The revisions listed in reverse chronological order, per dataset.
   * @param application This application.
   * @throws Exception For issues.
   */
  private static void downloadRecords(Map<String, Set<String>> records,
      Map<String, List<Revision>> revisions, Application application) throws Exception {
    try (final UISClient uisClient = new UISClient(application.getProperties().ecloudMcsBaseUrl,
        application.getProperties().ecloudUsername, application.getProperties().ecloudPassword,
        10_000, 10_000);
        final RecordServiceClient recordServiceClient = new RecordServiceClient(
            application.getProperties().ecloudMcsBaseUrl,
            application.getProperties().ecloudUsername, application.getProperties().ecloudPassword,
            10_000, 10_000);
        final FileServiceClient fileServiceClient = new FileServiceClient(
            application.getProperties().ecloudMcsBaseUrl,
            application.getProperties().ecloudUsername, application.getProperties().ecloudPassword,
            10_000, 10_000);
        final ExecutorService executor = Executors.newFixedThreadPool(8)) {
      final AtomicInteger counter = new AtomicInteger();
      LOGGER.info("Writing record contents ...");
      for (Map.Entry<String, Set<String>> datasetEntry : records.entrySet()) {
        final String dataset = datasetEntry.getKey();
        for (String record : datasetEntry.getValue()) {
          executor.submit(() -> {
            try {
              final String recordContents = getRecordFromECloud(record, revisions.get(dataset),
                  application, uisClient, recordServiceClient, fileServiceClient);
              if (recordContents != null) {
                saveRecordToFile(dataset, record, recordContents);
              }
              if (counter.incrementAndGet() % 10 == 0) {
                LOGGER.info("  {} records done.", counter.get());
              }
            } catch (Exception e) {
              LOGGER.warn("Error while writing record contents.", e);
            }
          });
        }
      }
      executor.shutdown();
      if (!executor.awaitTermination(100, TimeUnit.DAYS)) {
        LOGGER.error("Timed out waiting for executor to terminate.");
      }
      LOGGER.info("Total of {} records written.", counter.get());
    }
  }

  /**
   * @param record              The record ID
   * @param datasetRevisions    The known revisions, in reverse order.
   * @param application         This application.
   * @param uisClient           eCloud client.
   * @param recordServiceClient eCloud client.
   * @param fileServiceClient   eCloud client.
   * @return The record contents, or null if record could not be found.
   * @throws Exception For issues.
   */
  private static String getRecordFromECloud(String record, List<Revision> datasetRevisions,
      Application application, UISClient uisClient, RecordServiceClient recordServiceClient,
      FileServiceClient fileServiceClient) throws Exception {

    // Get the eCloud ID.
    final String ecloudId;
    try {
      ecloudId = uisClient.getCloudId(application.getProperties().ecloudProvider,
          record).getId();
    } catch (CloudException e) {
      if (e.getCause() instanceof RecordDoesNotExistException) {
        LOGGER.error("Could not find record ID in eCloud: {}.", record);
        return null;
      } else {
        throw e;
      }
    }

    // Get the record contents.
    for (Revision revision : datasetRevisions) {
      // Check all revisions: most recent one with the record is the one we need.
      final String recordContents = getRecordByEcloudIdAndRevision(revision, ecloudId,
          recordServiceClient, fileServiceClient);
      if (recordContents != null) {
        return recordContents;
      }
    }
    LOGGER.error("Could not find record in eCloud: {}.", record);
    return null;
  }

  /**
   * @param revision            The revision of the record.
   * @param ecloudId            The eCloud ID of the record.
   * @param recordServiceClient A client object for eCloud.
   * @param fileServiceClient   A client object for eCloud.
   * @return The record contents, or null if record does not exist for this revision.
   * @throws Exception For issues.
   */
  private static String getRecordByEcloudIdAndRevision(Revision revision, String ecloudId,
      RecordServiceClient recordServiceClient, FileServiceClient fileServiceClient)
      throws Exception {

    // Get the representation(s) for the given combination of plugin and record ID.
    final List<Representation> representations = recordServiceClient
        .getRepresentationsByRevision(ecloudId, MetisPlugin.getRepresentationName(), revision);
    if (representations == null || representations.isEmpty()) {
      return null;
    }
    final Representation representation = representations.getFirst();

    // Perform checks on the file lists.
    if (representation.getFiles() == null || representation.getFiles().isEmpty()) {
      return null;
    }
    final File file = representation.getFiles().getFirst();

    // Obtain the file contents belonging to this representation version.
    final InputStream inputStream = fileServiceClient.getFile(file.getContentUri().toString());
    return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
  }

  /**
   * Saves the record to the destination directory.
   *
   * @param dataset        The dataset ID.
   * @param recordId       The record ID.
   * @param recordContents The record contents.
   */
  private static void saveRecordToFile(String dataset, String recordId, String recordContents)
      throws IOException {
    final Path file = getFile(dataset, recordId);
    if (!Files.exists(file.getParent())) {
      synchronized (Main.class) {
        if (!Files.exists(file.getParent())) {
          Files.createDirectory(file.getParent());
        }
      }
    }
    Files.writeString(file, recordContents, StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING);
  }

  /**
   * @param trimmedLine The trimmed line (expected to contain just a record ID).
   * @return Dataset ID extracted from the record ID.
   */
  private static String getDatasetId(String trimmedLine) {
    final int datasetEnd = trimmedLine.indexOf('/', 1);
    if (datasetEnd <= 1 || datasetEnd != trimmedLine.lastIndexOf('/')) {
      return null;
    }
    return trimmedLine.substring(1, datasetEnd);
  }

  /**
   * @param dataset  The dataset ID
   * @param recordId The record ID
   * @return The file path to save the record.
   */
  private static Path getFile(String dataset, String recordId) {
    final Path directory = Path.of(DESTINATION_DIRECTORY, dataset);
    return directory.resolve(recordId.substring(recordId.lastIndexOf('/') + 1) + ".xml");
  }
}
