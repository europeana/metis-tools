package eu.europeana.metis.technical.metadata.generation.utilities;

import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import com.amazonaws.services.s3.AmazonS3;
import eu.europeana.metis.mediaprocessing.AbstractMediaProcessorPool.MediaExtractorPool;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.UrlType;
import eu.europeana.metis.technical.metadata.generation.model.DatasetFileStatus;
import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.model.Mode;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailFileStatus;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailWrapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
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
  private final MongoDao mongoDao;
  private final AmazonS3 amazonS3Client;
  private final String s3Bucket;
  private final MediaExtractorPool mediaExtractorPool;
  private final Mode mode;
  private final int parallelThreadsPerFile;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  MediaExtractorForFile(File datasetFile, MongoDao mongoDao, AmazonS3 amazonS3Client,
      String s3Bucket, MediaProcessorFactory mediaProcessorFactory, Mode mode,
      int parallelThreadsPerFile) {
    this.datasetFile = datasetFile;
    this.mongoDao = mongoDao;
    this.amazonS3Client = amazonS3Client;
    this.s3Bucket = s3Bucket;
    this.mediaExtractorPool = new MediaExtractorPool(mediaProcessorFactory);
    this.mode = mode;
    this.parallelThreadsPerFile = parallelThreadsPerFile;

    threadPool = Executors.newFixedThreadPool(parallelThreadsPerFile);
    completionService = new ExecutorCompletionService<>(threadPool);
  }

  @Override
  public Void call() throws InterruptedException {
    return parseMediaForFile(datasetFile);
  }

  private Void parseMediaForFile(File datasetFile) throws InterruptedException {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Parsing: {}", datasetFile);

    InputStream inputStream;
    try {
      inputStream = getInputStreamForFilePath(datasetFile);
    } catch (IOException e) {
      LOGGER.warn(EXECUTION_LOGS_MARKER, "Something went wrong when reading file: {}", datasetFile,
          e);
      return null;
    }

    final DatasetFileStatus datasetFileStatus = retrieveCorrectDatasetFileStatusModeBased(
        datasetFile.getName());
    //Exit if we cannot determine the file status
    if (datasetFileStatus == null) {
      return null;
    }
    int lineIndex = datasetFileStatus.getLineReached();
    try (Scanner scanner = new Scanner(inputStream, "UTF-8")) {
      //Bypass lines until the reached one, from a previous execution
      if (moveScannerToLine(scanner, datasetFileStatus)) {
        final Set<Integer> nonRegisteredLines = new HashSet<>();
        int threadCounter = 0;
        while (scanner.hasNextLine()) {

          // If all threads are busy, wait for the first one to become available.
          if (threadCounter >= parallelThreadsPerFile) {
            completionService.take();
            threadCounter--;
          }

          // Submit task for this line.
          lineIndex++;
          submitResource(datasetFileStatus, nonRegisteredLines, lineIndex, scanner.nextLine());
          threadCounter++;
        }

        // Wait for remaining threads to finish.
        for (int i = 0; i < threadCounter; i++) {
          completionService.take();
        }

        // Set file status for end of file.
        datasetFileStatus.setEndOfFileReached(true);
        mongoDao.storeFileStatusToDb(datasetFileStatus);
      }
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "Finished: {}", datasetFile);
    mediaExtractorPool.close();
    threadPool.shutdown();
    return null;
  }

  private DatasetFileStatus retrieveCorrectDatasetFileStatusModeBased(String fileName) {
    DatasetFileStatus fileStatus = getFileStatus(fileName);
    if (mode == Mode.UPLOAD_THUMBNAILS) {
      //Check if the generation of technical metadata has actually finished
      if (!fileStatus.isEndOfFileReached()) {
        LOGGER.info(EXECUTION_LOGS_MARKER,
            "FileStatus {} that has not been fully processed will not be processed for thumbnail upload..",
            fileStatus.getFileName());
        return null;
      }
      //Otherwise replace file status with thumbnail file status
      fileStatus = getThumbnailFileStatus(fileName);
    }
    return fileStatus;
  }

  private void submitResource(final DatasetFileStatus fileStatus,
      final Set<Integer> nonRegisteredLines,
      final int thisLineIndex, final String thisLine) {
    completionService.submit(() -> {

      // process resource
      if (mode == Mode.UPLOAD_THUMBNAILS) {
        thumbnailUpload(thisLine);
      } else {
        processResource(thisLine);
      }

      // Set file status - synchronized to protect the set and the file status
      synchronized (this) {

        // Add the line to the non-registered lines.
        nonRegisteredLines.add(thisLineIndex);

        // Increase the file status for all consecutive numbers that are reached.
        while (nonRegisteredLines.contains(fileStatus.getLineReached() + 1)) {

          // Remove from the non-registered numbers, and set the file status.
          nonRegisteredLines.remove(fileStatus.getLineReached() + 1);
          fileStatus.setLineReached(fileStatus.getLineReached() + 1);

          // Occasionally, output progress.
          if (fileStatus.getLineReached() % 100 == 0) {
            LOGGER.info(EXECUTION_LOGS_MARKER, "Processing file: {}, reached line {}",
                datasetFile.getName(), fileStatus.getLineReached());
          }
        }

        // Save file status.
        mongoDao.storeFileStatusToDb(fileStatus);
      }

      // Done.
      return null;
    });
  }

  private void thumbnailUpload(String resourceUrl) {
    final TechnicalMetadataWrapper technicalMetadataWrapper = mongoDao
        .getTechnicalMetadataWrapper(resourceUrl);
    if (technicalMetadataWrapper == null || technicalMetadataWrapper.getThumbnailWrappers() == null
        || technicalMetadataWrapper.getThumbnailWrappers().isEmpty()) {
      LOGGER.info("Resource does not have thumbnails: {}", resourceUrl);
    } else {
      final boolean successfulOperation = storeThumbnailsToS3(amazonS3Client, s3Bucket,
          technicalMetadataWrapper.getThumbnailWrappers());
      if (successfulOperation) {
        mongoDao.removeThumbnailsFromTechnicalMetadataWrapper(technicalMetadataWrapper);
      }
    }
  }

  private void processResource(String resourceUrl) {
    LOGGER.info(EXECUTION_LOGS_MARKER, "Processing datasetFile {},  resource: {}",
        datasetFile.getName(), resourceUrl);
    //Should this resource be processed according to status and configuration parameters
    if (eligibleForProcessing(resourceUrl)) {
      try (final ResourceExtractionResult resourceExtractionResult = performMediaExtraction(
          resourceUrl)) {
        mongoDao.storeMediaResultInDb(resourceExtractionResult);
      } catch (Exception e) {
        LOGGER.warn(EXECUTION_LOGS_MARKER, "Media extraction failed for resourceUrl {}",
            resourceUrl, e);
        mongoDao.storeFailedMediaInDb(resourceUrl, exceptionStacktraceToString(e));
      }
    } else {
      LOGGER.info(EXECUTION_LOGS_MARKER, "ResourceUrl already exists in db: {}", resourceUrl);
    }
  }

  private static String exceptionStacktraceToString(Exception e) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    e.printStackTrace(ps);
    ps.close();
    return baos.toString();
  }

  public static InputStream getInputStreamForFilePath(File datasetFile) throws IOException {
    InputStream inputStream;
    if (isGZipped(datasetFile)) {
      inputStream = new GZIPInputStream(new FileInputStream(datasetFile));
    } else {
      inputStream = new FileInputStream(datasetFile);
    }
    return inputStream;
  }

  private ResourceExtractionResult performMediaExtraction(String resourceUrl)
      throws MediaExtractionException, MediaProcessorException {
    //Use all url types to get metadata and thumbnails for all. Later we decide what to use.
    RdfResourceEntry resourceEntry = new RdfResourceEntry(resourceUrl,
        new ArrayList<>(UrlType.URL_TYPES_FOR_MEDIA_EXTRACTION));
    // Perform metadata extraction
    LOGGER.info(EXECUTION_LOGS_MARKER, "Processing: {}", resourceEntry.getResourceUrl());
    return mediaExtractorPool.processTask(resourceEntry);
  }

  private boolean eligibleForProcessing(String resourceUrl) {
    final TechnicalMetadataWrapper technicalMetadataWrapper = mongoDao
        .getTechnicalMetadataWrapperFieldProjection(resourceUrl);
    final boolean isMetadataNonNullAndRetryFailedResourcesTrue =
        technicalMetadataWrapper != null && !technicalMetadataWrapper.isSuccessExtraction()
            && Mode.RETRY_FAILED.equals(mode);

    return technicalMetadataWrapper == null || isMetadataNonNullAndRetryFailedResourcesTrue;
  }

  private boolean moveScannerToLine(Scanner scanner, DatasetFileStatus fileStatus) {
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
      mongoDao.storeFileStatusToDb(fileStatus);
    }

    //Re-store the status in db
    return fileStatus;
  }

  private ThumbnailFileStatus getThumbnailFileStatus(String fileName) {
    //From Mongo
    ThumbnailFileStatus thumbnailFileStatus = mongoDao.getThumbnailFileStatus(fileName);

    //Or reset
    if (thumbnailFileStatus == null) {
      thumbnailFileStatus = new ThumbnailFileStatus(fileName, 0);
      mongoDao.storeFileStatusToDb(thumbnailFileStatus);
    }

    return thumbnailFileStatus;
  }

  private static boolean isGZipped(File file) {
    int magic = 0;
    try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
      magic = raf.read() & 0xff | ((raf.read() << 8) & 0xff00);
    } catch (RuntimeException | IOException e) {
      LOGGER.error("Could not determine if file is gzip", e);
    }
    return magic == GZIPInputStream.GZIP_MAGIC;
  }

  static boolean storeThumbnailsToS3(AmazonS3 amazonS3Client, String s3Bucket,
      List<ThumbnailWrapper> thumbnailWrappers) {
    boolean successfulOperation = true;
    for (ThumbnailWrapper thumbnailWrapper : thumbnailWrappers) {
      //If the thumbnail already exists(e.g. from a previous execution of the script), avoid sending it again
      LOGGER.info(EXECUTION_LOGS_MARKER, "Checking if thumbnail already exists in s3 with name: {}",
          thumbnailWrapper.getTargetName());
      if (!doesThumbnailExistInS3(amazonS3Client, s3Bucket, thumbnailWrapper.getTargetName())) {
        try (InputStream stream = new ByteArrayInputStream(thumbnailWrapper.getThumbnailBytes())) {
          amazonS3Client.putObject(s3Bucket, thumbnailWrapper.getTargetName(), stream, null);
          LOGGER.info(EXECUTION_LOGS_MARKER, "Sent item to S3 with name: {}",
              thumbnailWrapper.getTargetName());
        } catch (Exception e) {
          LOGGER.error(EXECUTION_LOGS_MARKER,
              "Error while uploading {} to S3 in Bluemix. The full error message is: {} because of: ",
              thumbnailWrapper.getTargetName(), e);
          successfulOperation = false;
        }
      }
    }
    return successfulOperation;
  }

  static boolean doesThumbnailExistInS3(AmazonS3 amazonS3Client, String s3Bucket,
      String targetNameLarge) {
    return amazonS3Client.doesObjectExist(s3Bucket, targetNameLarge);
  }

}
