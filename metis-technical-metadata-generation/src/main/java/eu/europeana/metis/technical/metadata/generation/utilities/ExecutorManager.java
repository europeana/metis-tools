package eu.europeana.metis.technical.metadata.generation.utilities;

import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;
import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  public static final String PROCESSED_FILES_STR = "Processed files: {}";
  private final MongoDao mongoDao;
  private final File directoryWithResourcesPerDataset;
  private final MediaExtractor mediaExtractor;
  private final boolean startFromBeginningOfFiles;
  private final boolean retryFailedResources;
  private final int maxParallelThreads;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  public ExecutorManager(Datastore datastore, int maxParallelThreads,
      File directoryWithResourcesPerDataset,
      boolean startFromBeginningOfFiles, boolean retryFailedResources)
      throws MediaProcessorException {
    this.mongoDao = new MongoDao(datastore);
    this.maxParallelThreads = maxParallelThreads;
    this.directoryWithResourcesPerDataset = directoryWithResourcesPerDataset;
    final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
    this.mediaExtractor = processorFactory.createMediaExtractor();
    this.startFromBeginningOfFiles = startFromBeginningOfFiles;
    this.retryFailedResources = retryFailedResources;

    threadPool = Executors.newFixedThreadPool(maxParallelThreads);
    completionService = new ExecutorCompletionService<>(threadPool);
  }

  public void startTechnicalMetadataGeneration() throws IOException, InterruptedException {
    final File[] filesPerDataset = directoryWithResourcesPerDataset.listFiles();
    final boolean allFiles =
        filesPerDataset != null && Arrays.stream(filesPerDataset).allMatch(File::isFile);
    if (!allFiles) {
      LOGGER.error(EXECUTION_LOGS_MARKER, "There are non file items under {}", directoryWithResourcesPerDataset);
      throw new IOException("There are non file items under the specified directory");
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "Total files to process: {}", filesPerDataset.length);

    int threadCounter = 0;
    int processedFiles = 0;
    for (File datasetFile : filesPerDataset) {
      final MediaExtractorForFile mediaExtractorForFile = new MediaExtractorForFile(datasetFile,
          mongoDao, mediaExtractor, startFromBeginningOfFiles, retryFailedResources);
      if (threadCounter >= maxParallelThreads) {
        completionService.take();
        threadCounter--;
        processedFiles++;
        LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
      } else {
        completionService.submit(mediaExtractorForFile);
        threadCounter++;
      }
    }

    //Final cleanup of futures
    for (int i = 0; i < threadCounter; i++) {
      completionService.take();
      processedFiles++;
      LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "Total failed resources in db {}", mongoDao.getTotalFailedResources());

    LOGGER.info(STATISTICS_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
    LOGGER.info(STATISTICS_LOGS_MARKER, "Total processed resources in db {}", mongoDao.getTotalProcessedResources());
    LOGGER.info(STATISTICS_LOGS_MARKER, "Total failed resources in db {}", mongoDao.getTotalFailedResources());
  }

  public void close() throws IOException {
    threadPool.shutdown();
    mediaExtractor.close();
  }

}
