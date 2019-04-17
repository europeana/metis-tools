package eu.europeana.metis.technical.metadata.generation.utilities;

import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.model.UrlType;
import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;
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
  private final boolean retryFailedResources;

  public ExecutorManager(Datastore datastore, File directoryWithResourcesPerDataset,
      boolean retryFailedResources)
      throws MediaProcessorException {
    this.mongoDao = new MongoDao(datastore);
    this.directoryWithResourcesPerDataset = directoryWithResourcesPerDataset;
    final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
    this.mediaExtractor = processorFactory.createMediaExtractor();
    this.retryFailedResources = retryFailedResources;
  }

  public void startTechnicalMetadataGeneration() throws IOException {
    final File[] filesPerDataset = this.directoryWithResourcesPerDataset.listFiles();
    final boolean allFiles =
        filesPerDataset != null && Arrays.stream(filesPerDataset).allMatch(File::isFile);
    if (!allFiles) {
      LOGGER.error("There are non file items under {}", this.directoryWithResourcesPerDataset);
      throw new IOException("There are non file items under the specified directory");
    }

    for (File datasetFile : filesPerDataset) {
      parseMediaForFile(datasetFile);
    }
  }

  private void parseMediaForFile(File datasetFile) throws IOException {
    LOGGER.info("Starting parsing file {}", datasetFile);
    final FileStatus fileStatus = getFileStatus(datasetFile.getName(), retryFailedResources);

    int lineIndex = fileStatus.getLineReached();
    try (Scanner scanner = new Scanner(datasetFile, "UTF-8")) {
      if (moveScannerToLine(scanner, fileStatus)) {
        while (scanner.hasNextLine()) {
          String resourceUrl = scanner.nextLine();
          //Only generate if non existent
          if (eligibleForProcessing(resourceUrl)) {
            final ResourceExtractionResult resourceExtractionResult;
            try {
              resourceExtractionResult = performMediaExtraction(resourceUrl);
              mongoDao.storeMediaResultInDb(resourceExtractionResult);
              clearThumbnails(resourceExtractionResult);
            } catch (MediaExtractionException e) {
              LOGGER.warn("Media extraction failed for resourceUrl {}", resourceUrl);
              mongoDao.storeFailedMediaInDb(resourceUrl);
            }
          } else {
            LOGGER.info("ResourceUrl already exists in db: {}", resourceUrl);
          }
          lineIndex++;
          fileStatus.setLineReached(lineIndex);
          mongoDao.storeFileStatusToDb(fileStatus);
        }
        fileStatus.setEndOfFileReached(true);
        mongoDao.storeFileStatusToDb(fileStatus);
      }
    }
  }

  private boolean eligibleForProcessing(String resourceUrl) {
    final TechnicalMetadataWrapper technicalMetadataWrapper = mongoDao
        .getTechnicalMetadataWrapper(resourceUrl);
    return technicalMetadataWrapper == null || (!technicalMetadataWrapper.isSuccessExtraction()
        && retryFailedResources);
  }

  private boolean moveScannerToLine(Scanner scanner, FileStatus fileStatus) {
    if (fileStatus.isEndOfFileReached()) {
      LOGGER.warn("On a previous execution, we have already reached the end of file {}",
          fileStatus.getFileName());
      return false;
    }
    final int lineReached = fileStatus.getLineReached();
    LOGGER.info("Will try to move to the line {} and continue from there for file {}", lineReached,
        fileStatus.getFileName());
    for (int i = 0; i < lineReached; i++) {
      if (scanner.hasNextLine()) {
        scanner.nextLine();
      } else {
        return false;
      }
    }
    return true;
  }

  private FileStatus getFileStatus(String fileName, boolean retryFailedResources) {
    FileStatus fileStatus = mongoDao.getFileStatus(fileName);
    if (fileStatus == null) {
      fileStatus = new FileStatus(fileName, 0);
    } else if (retryFailedResources) {
      LOGGER.info(
          "Since retryFailedResources == true, then we'll start from the beginning of the file");
      fileStatus.setEndOfFileReached(false);
      fileStatus.setLineReached(0);
    }
    mongoDao.storeFileStatusToDb(fileStatus);
    return fileStatus;
  }

  private void clearThumbnails(ResourceExtractionResult resourceExtractionResult)
      throws IOException {
    //At the end clear thumbnails
    LOGGER.info("Removing thumbnails for {}.",
        resourceExtractionResult.getMetadata().getResourceUrl());
    if (resourceExtractionResult.getThumbnails() != null) {
      for (Thumbnail thumbnail : resourceExtractionResult.getThumbnails()) {
        thumbnail.close();
      }
    }
  }

  private ResourceExtractionResult performMediaExtraction(String resourceUrl)
      throws MediaExtractionException {
    //Use all url types to get metadata and thumbnails for all. Later we decide what to use.
    RdfResourceEntry resourceEntry = new RdfResourceEntry(resourceUrl,
        Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY, UrlType.IS_SHOWN_AT));
    // Perform metadata extraction
    LOGGER.info("Processing: " + resourceEntry.getResourceUrl());
    return mediaExtractor.performMediaExtraction(resourceEntry);
  }

  public void close() throws IOException {
    this.mediaExtractor.close();
  }
}
