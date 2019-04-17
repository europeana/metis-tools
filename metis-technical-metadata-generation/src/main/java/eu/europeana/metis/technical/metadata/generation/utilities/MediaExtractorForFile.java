package eu.europeana.metis.technical.metadata.generation.utilities;

import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
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
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class MediaExtractorForFile implements Callable<Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MediaExtractorForFile.class);
  private final File datasetFile;
  private static MongoDao mongoDao;
  private static MediaExtractor mediaExtractor;
  private static boolean startFromBeginningOfFiles;
  private static boolean retryFailedResources;

  MediaExtractorForFile(File datasetFile, MongoDao mongoDao, MediaExtractor mediaExtractor,
      boolean startFromBeginningOfFiles, boolean retryFailedResources) {
    this.datasetFile = datasetFile;
    MediaExtractorForFile.mongoDao = mongoDao;
    MediaExtractorForFile.mediaExtractor = mediaExtractor;
    MediaExtractorForFile.startFromBeginningOfFiles = startFromBeginningOfFiles;
    MediaExtractorForFile.retryFailedResources = retryFailedResources;
  }

  @Override
  public Void call() throws Exception {
    return parseMediaForFile(datasetFile);
  }

  static Void parseMediaForFile(File datasetFile) throws IOException {
    LOGGER.info("Parsing: {}", datasetFile);
    final FileStatus fileStatus = getFileStatus(datasetFile.getName(), startFromBeginningOfFiles,
        retryFailedResources);

    int lineIndex = fileStatus.getLineReached();
    try (Scanner scanner = new Scanner(datasetFile, "UTF-8")) {
      if (moveScannerToLine(scanner, fileStatus)) {
        while (scanner.hasNextLine()) {
          String resourceUrl = scanner.nextLine();
          LOGGER.info("Processing resource: {}", resourceUrl);
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

          if (lineIndex % 100 == 0) {
            LOGGER.info("File: {}, reached line {}", datasetFile.getName(), lineIndex);
          }
        }
        fileStatus.setEndOfFileReached(true);
        mongoDao.storeFileStatusToDb(fileStatus);
      }
    }
    LOGGER.info("Finished: {}", datasetFile);
    return null;
  }

  private static ResourceExtractionResult performMediaExtraction(String resourceUrl)
      throws MediaExtractionException {
    //Use all url types to get metadata and thumbnails for all. Later we decide what to use.
    RdfResourceEntry resourceEntry = new RdfResourceEntry(resourceUrl,
        Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY, UrlType.IS_SHOWN_AT));
    // Perform metadata extraction
    LOGGER.info("Processing: {}", resourceEntry.getResourceUrl());
    return mediaExtractor.performMediaExtraction(resourceEntry);
  }

  private static void clearThumbnails(ResourceExtractionResult resourceExtractionResult)
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

  private static boolean eligibleForProcessing(String resourceUrl) {
    final TechnicalMetadataWrapper technicalMetadataWrapper = mongoDao
        .getTechnicalMetadataWrapper(resourceUrl);
    final boolean isMetadataNullAndRetryFailedResourceFalse =
        technicalMetadataWrapper == null && !retryFailedResources;
    final boolean isMetadataNonNullAndRetryFailedResourcesTrue =
        technicalMetadataWrapper != null && !technicalMetadataWrapper.isSuccessExtraction()
            && retryFailedResources;

    return isMetadataNullAndRetryFailedResourceFalse
        || isMetadataNonNullAndRetryFailedResourcesTrue;
  }

  private static boolean moveScannerToLine(Scanner scanner, FileStatus fileStatus) {
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

  private static FileStatus getFileStatus(String fileName, boolean startFromBeginningOfFiles,
      boolean retryFailedResources) {
    FileStatus fileStatus = mongoDao.getFileStatus(fileName);
    if (fileStatus == null) {
      fileStatus = new FileStatus(fileName, 0);
    } else if (startFromBeginningOfFiles || retryFailedResources) {
      LOGGER.info(
          "Since startFromBeginningOfFiles or retryFailedResources is true, then we'll start from the beginning of the file");
      fileStatus.setEndOfFileReached(false);
      fileStatus.setLineReached(0);
    }
    mongoDao.storeFileStatusToDb(fileStatus);
    return fileStatus;
  }

}
