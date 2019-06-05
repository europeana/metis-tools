package eu.europeana.metis.technical.metadata.generation.utilities;

import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;
import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.STATISTICS_LOGS_MARKER;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.technical.metadata.generation.model.Mode;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.StringUtils;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the {@link ExecutorService} class and handles the parallelization of the tasks.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String PROCESSED_FILES_STR = "Processed files: {}";
  private final MongoDao mongoDao;
  private final File directoryWithResourcesPerDataset;
  private final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
  private AmazonS3 amazonS3Client;
  private String s3Bucket;
  private final Mode mode;
  private final int maxParallelThreads;
  private final int parallelThreadsPerFile;
  private final int startFromFileIndexInDirectory;
  private final int endAtFileIndexInDirectory;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  public ExecutorManager(Datastore datastore, PropertiesHolder propertiesHolder) {
    this.mongoDao = new MongoDao(datastore);
    this.maxParallelThreads = propertiesHolder.maxParallelThreads;
    this.parallelThreadsPerFile = propertiesHolder.parallelThreadsPerFile;
    this.startFromFileIndexInDirectory = propertiesHolder.startFromFileIndexInDirectory;
    this.endAtFileIndexInDirectory = propertiesHolder.endAtFileIndexInDirectory;
    this.directoryWithResourcesPerDataset = propertiesHolder.directoryWithResourcesPerDatasetPath;
    this.mode = propertiesHolder.mode;

    //S3
    if (StringUtils.isNotBlank(propertiesHolder.s3AccessKey) && StringUtils
        .isNotBlank(propertiesHolder.s3SecretKey) && StringUtils
        .isNotBlank(propertiesHolder.s3Bucket)) {
      this.amazonS3Client = new AmazonS3Client(new BasicAWSCredentials(
          propertiesHolder.s3AccessKey,
          propertiesHolder.s3SecretKey));
      amazonS3Client.setEndpoint(propertiesHolder.s3Endpoint);
      this.s3Bucket = propertiesHolder.s3Bucket;
    }

    threadPool = Executors.newFixedThreadPool(maxParallelThreads);
    completionService = new ExecutorCompletionService<>(threadPool);

    processorFactory.setResourceConnectTimeout(propertiesHolder.resourceConnectTimeout);
    processorFactory.setResourceSocketTimeout(propertiesHolder.resourceSocketTimeout);
  }

  public static File[] getAllFiles(File directoryWithResourcesPerDataset) throws IOException {
    final File[] filesPerDataset = directoryWithResourcesPerDataset.listFiles();
    final boolean allFiles =
        filesPerDataset != null && Arrays.stream(filesPerDataset).allMatch(File::isFile);
    if (!allFiles) {
      LOGGER.error(EXECUTION_LOGS_MARKER, "There are non file items under {}",
          directoryWithResourcesPerDataset);
      throw new IOException("There are non file items under the specified directory");
    }
    Arrays.sort(filesPerDataset, Comparator.comparing(File::getName));
    LOGGER.info(EXECUTION_LOGS_MARKER, "Total files to process: {}", filesPerDataset.length);
    return filesPerDataset;
  }

  public void startTechnicalMetadataGeneration() throws IOException, InterruptedException {

    final File[] filesPerDataset = getAllFiles(directoryWithResourcesPerDataset);

    int threadCounter = 0;
    int processedFiles = 0;
    int fileIndexInDirectory = 0;
    for (File datasetFile : filesPerDataset) {
      fileIndexInDirectory++;
      if (fileIndexInDirectory < startFromFileIndexInDirectory) {
        continue;
      }
      if (fileIndexInDirectory > endAtFileIndexInDirectory) {
        break;
      }
      final MediaExtractorForFile mediaExtractorForFile = new MediaExtractorForFile(datasetFile,
          mongoDao, amazonS3Client, s3Bucket, processorFactory, mode, parallelThreadsPerFile);
      if (threadCounter >= maxParallelThreads) {
        completionService.take();
        threadCounter--;
        processedFiles++;
        LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
      }
      completionService.submit(mediaExtractorForFile);
      threadCounter++;
    }

    //Final cleanup of futures
    for (int i = 0; i < threadCounter; i++) {
      completionService.take();
      processedFiles++;
      LOGGER.info(EXECUTION_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "Total failed resources in db {}",
        mongoDao.getTotalFailedResources());

    LOGGER.info(STATISTICS_LOGS_MARKER, PROCESSED_FILES_STR, processedFiles);
    LOGGER.info(STATISTICS_LOGS_MARKER, "Total processed resources in db {}",
        mongoDao.getTotalProcessedResources());
    LOGGER.info(STATISTICS_LOGS_MARKER, "Total failed resources in db {}",
        mongoDao.getTotalFailedResources());
  }

  public void close() {
    threadPool.shutdown();
  }

}
