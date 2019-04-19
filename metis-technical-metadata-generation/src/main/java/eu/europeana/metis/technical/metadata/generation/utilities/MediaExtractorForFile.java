package eu.europeana.metis.technical.metadata.generation.utilities;

import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.model.UrlType;
import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.model.Mode;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Callable} that will go through a initialized dataset file and parse each resource per
 * line.
 * <p>This implementation saves the status of the file in the database so that on a subsequent
 * execution, depending on the configuration, it will skip an X amount of lines. It can re-process
 * failed resources or reset the reading of the file from the beginning, if set in the
 * configuration.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class MediaExtractorForFile implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MediaExtractorForFile.class);
  private final File datasetFile;
  private MongoDao mongoDao;
  private MediaExtractor mediaExtractor;
  private Mode mode;

  MediaExtractorForFile(File datasetFile, MongoDao mongoDao, MediaExtractor mediaExtractor,
      Mode mode) {
    this.datasetFile = datasetFile;
    this.mongoDao = mongoDao;
    this.mediaExtractor = mediaExtractor;
    this.mode = mode;
  }

  @Override
  public Void call() throws Exception {
    return parseMediaForFile(datasetFile);
  }

  private Void parseMediaForFile(File datasetFile) throws IOException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Parsing: {}", datasetFile);
    final FileStatus fileStatus = getFileStatus(datasetFile.getName());

    int lineIndex = fileStatus.getLineReached();
    try (Scanner scanner = new Scanner(datasetFile, "UTF-8")) {
      //Bypass lines until the reached one, from a previous execution
      if (moveScannerToLine(scanner, fileStatus)) {
        while (scanner.hasNextLine()) {
          String resourceUrl = scanner.nextLine();
          LOGGER.info(EXECUTION_LOGS_MARKER, "Processing resource: {}", resourceUrl);
          //Should this resource be processed according to status and configuration parameters
          if (eligibleForProcessing(resourceUrl)) {
            try (final ResourceExtractionResult resourceExtractionResult = performMediaExtraction(
                resourceUrl)) {
              mongoDao.storeMediaResultInDb(resourceExtractionResult);
            } catch (MediaExtractionException e) {
              LOGGER.warn(EXECUTION_LOGS_MARKER, "Media extraction failed for resourceUrl {}",
                  resourceUrl);
              mongoDao.storeFailedMediaInDb(resourceUrl);
            }
          } else {
            LOGGER.info(EXECUTION_LOGS_MARKER, "ResourceUrl already exists in db: {}", resourceUrl);
          }
          lineIndex++;
          fileStatus.setLineReached(lineIndex);
          mongoDao.storeFileStatusToDb(fileStatus);

          if (lineIndex % 100 == 0) {
            LOGGER.info(EXECUTION_LOGS_MARKER, "Processing file: {}, reached line {}",
                datasetFile.getName(), lineIndex);
          }
        }
        fileStatus.setEndOfFileReached(true);
        mongoDao.storeFileStatusToDb(fileStatus);
      }
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "Finished: {}", datasetFile);
    return null;
  }

  private ResourceExtractionResult performMediaExtraction(String resourceUrl)
      throws MediaExtractionException {
    //Use all url types to get metadata and thumbnails for all. Later we decide what to use.
    RdfResourceEntry resourceEntry = new RdfResourceEntry(resourceUrl,
        new ArrayList<>(UrlType.URL_TYPES_FOR_MEDIA_EXTRACTION));
    // Perform metadata extraction
    LOGGER.info(EXECUTION_LOGS_MARKER, "Processing: {}", resourceEntry.getResourceUrl());
    return mediaExtractor.performMediaExtraction(resourceEntry);
  }

  private void clearThumbnails(ResourceExtractionResult resourceExtractionResult)
      throws IOException {
    //At the end clear thumbnails
    LOGGER.info(EXECUTION_LOGS_MARKER, "Removing thumbnails for {}.",
        resourceExtractionResult.getMetadata().getResourceUrl());
    if (resourceExtractionResult.getThumbnails() != null) {
      for (Thumbnail thumbnail : resourceExtractionResult.getThumbnails()) {
        thumbnail.close();
      }
    }
  }

  private boolean eligibleForProcessing(String resourceUrl) {
    final TechnicalMetadataWrapper technicalMetadataWrapper = mongoDao
        .getTechnicalMetadataWrapper(resourceUrl);
    final boolean isMetadataNonNullAndRetryFailedResourcesTrue =
        technicalMetadataWrapper != null && !technicalMetadataWrapper.isSuccessExtraction()
            && Mode.START_FROM_BEGINNING_RETRY_FAILED.equals(mode);

    return technicalMetadataWrapper == null || isMetadataNonNullAndRetryFailedResourcesTrue;
  }

  private boolean moveScannerToLine(Scanner scanner, FileStatus fileStatus) {
    if (fileStatus.isEndOfFileReached()) {
      LOGGER.warn(EXECUTION_LOGS_MARKER,
          "On a previous execution, we have already reached the end of file {}",
          fileStatus.getFileName());
      return false;
    }
    final int lineReached = fileStatus.getLineReached();
    LOGGER.info(EXECUTION_LOGS_MARKER,
        "Will try to move to line {} and continue from there for file: {}", lineReached,
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

  private FileStatus getFileStatus(String fileName) {
    //From Mongo
    FileStatus fileStatus = mongoDao.getFileStatus(fileName);

    //Or reset
    if (fileStatus == null) {
      fileStatus = new FileStatus(fileName, 0);
    } else if (Mode.START_FROM_BEGINNING_IGNORE_PROCESSED.equals(mode)
        || Mode.START_FROM_BEGINNING_RETRY_FAILED.equals(mode)) {
      LOGGER.info(EXECUTION_LOGS_MARKER,
          "Since startFromBeginningOfFiles or retryFailedResources is true, then we'll start from the beginning of the file");
      fileStatus.setEndOfFileReached(false);
      fileStatus.setLineReached(0);
    }
    //Re-store the status in db
    mongoDao.storeFileStatusToDb(fileStatus);
    return fileStatus;
  }

}
