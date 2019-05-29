package eu.europeana.metis.technical.metadata.generation.utilities;

import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.metis.mediaprocessing.AbstractMediaProcessorPool.MediaExtractorPool;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.UrlType;
import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.model.Mode;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashSet;
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
  private final MediaExtractorPool mediaExtractorPool;
  private final Mode mode;
  private final int parallelThreadsPerFile;

  private final ExecutorService threadPool;
  private final ExecutorCompletionService<Void> completionService;

  MediaExtractorForFile(File datasetFile, MongoDao mongoDao,
      MediaProcessorFactory mediaProcessorFactory, Mode mode, int parallelThreadsPerFile) {
    this.datasetFile = datasetFile;
    this.mongoDao = mongoDao;
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
      LOGGER.warn(EXECUTION_LOGS_MARKER, "Something went wrong when reading file: {}", datasetFile, e);
      return null;
    }

    final FileStatus fileStatus = getFileStatus(datasetFile.getName());
    int lineIndex = fileStatus.getLineReached();
    try (Scanner scanner = new Scanner(inputStream, "UTF-8")) {
      //Bypass lines until the reached one, from a previous execution
      if (moveScannerToLine(scanner, fileStatus)) {
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
          submitResource(fileStatus, nonRegisteredLines, lineIndex, scanner.nextLine());
          threadCounter++;
        }

        // Wait for remaining threads to finish.
        for (int i = 0; i < threadCounter; i++) {
          completionService.take();
        }

        // Set file status for end of file.
        fileStatus.setEndOfFileReached(true);
        mongoDao.storeFileStatusToDb(fileStatus);
      }
    }
    LOGGER.info(EXECUTION_LOGS_MARKER, "Finished: {}", datasetFile);
    mediaExtractorPool.close();
    threadPool.shutdown();
    return null;
  }

  private void submitResource(final FileStatus fileStatus, final Set<Integer> nonRegisteredLines,
      final int thisLineIndex, final String thisLine) {
    completionService.submit(() -> {

      // process resource
      processResource(thisLine);

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
        .getTechnicalMetadataWrapper(resourceUrl);
    final boolean isMetadataNonNullAndRetryFailedResourcesTrue =
        technicalMetadataWrapper != null && !technicalMetadataWrapper.isSuccessExtraction()
            && Mode.RETRY_FAILED.equals(mode);

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
      mongoDao.storeFileStatusToDb(fileStatus);
    }

    //Re-store the status in db
    return fileStatus;
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

}
