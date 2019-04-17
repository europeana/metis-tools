package eu.europeana.metis.technical.metadata.generation.utilities;

import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private final MongoDao mongoDao;
  private final File directoryWithResourcesPerDataset;
  private final MediaExtractor mediaExtractor;
  private final boolean startFromBeginningOfFiles;
  private final boolean retryFailedResources;

  public ExecutorManager(Datastore datastore, File directoryWithResourcesPerDataset,
      boolean startFromBeginningOfFiles, boolean retryFailedResources)
      throws MediaProcessorException {
    this.mongoDao = new MongoDao(datastore);
    this.directoryWithResourcesPerDataset = directoryWithResourcesPerDataset;
    final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
    this.mediaExtractor = processorFactory.createMediaExtractor();
    this.startFromBeginningOfFiles = startFromBeginningOfFiles;
    this.retryFailedResources = retryFailedResources;
  }

  public void startTechnicalMetadataGeneration() throws Exception {
    final File[] filesPerDataset = directoryWithResourcesPerDataset.listFiles();
    final boolean allFiles =
        filesPerDataset != null && Arrays.stream(filesPerDataset).allMatch(File::isFile);
    if (!allFiles) {
      LOGGER.error("There are non file items under {}", directoryWithResourcesPerDataset);
      throw new IOException("There are non file items under the specified directory");
    }

    for (File datasetFile : filesPerDataset) {
      final MediaExtractorForFile mediaExtractorForFile = new MediaExtractorForFile(datasetFile,
          mongoDao, mediaExtractor, startFromBeginningOfFiles, retryFailedResources);
      mediaExtractorForFile.call();
      //should be submitted in executorservice.
    }
  }

  public void close() throws IOException {
    mediaExtractor.close();
  }

}
