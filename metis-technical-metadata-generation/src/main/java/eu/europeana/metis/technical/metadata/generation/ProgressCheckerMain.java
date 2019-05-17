package eu.europeana.metis.technical.metadata.generation;

import static eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder.EXECUTION_LOGS_MARKER;

import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.utilities.ExecutorManager;
import eu.europeana.metis.technical.metadata.generation.utilities.MediaExtractorForFile;
import eu.europeana.metis.technical.metadata.generation.utilities.MongoDao;
import eu.europeana.metis.technical.metadata.generation.utilities.MongoInitializer;
import eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.logging.log4j.core.net.ssl.TrustStoreConfigurationException;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressCheckerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressCheckerMain.class);

  private static final String CONFIGURATION_FILE = "application.properties";
  private static final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);

  public static void main(String[] args) throws TrustStoreConfigurationException, IOException {

    // Initialize.
    LOGGER.info("Starting script - initializing connections.");
    final MongoInitializer mongoInitializer = TechnicalMetadataGenerationMain
        .prepareConfiguration();
    final Datastore datastore = TechnicalMetadataGenerationMain
        .createDatastore(mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);

    // Check the progress.
    checkProgress(datastore, 0, 190);
    checkProgress(datastore, 191, 204);
    checkProgress(datastore, 205, 325);
    checkProgress(datastore, 326, 1060);
    checkProgress(datastore, 1061, 2200);

    // Cleanup.
    LOGGER.info("Done.");
    mongoInitializer.close();
  }

  private static int getLinkCount(File datasetFile) throws IOException {
    try (InputStream inputStream = MediaExtractorForFile.getInputStreamForFilePath(datasetFile);
        Reader inputStreamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(inputStreamReader)) {
      int lines = 0;
      while (reader.readLine() != null) {
        lines++;
      }
      return lines;
    }
  }

  private static void checkProgress(Datastore datastore, int from, int to) throws IOException {
    final MongoDao mongoDao = new MongoDao(datastore);
    final File[] filesPerDataset = ExecutorManager
        .getAllFiles(propertiesHolder.directoryWithResourcesPerDatasetPath);


    long totalLinkCount = 0;
    long processedLinkCount = 0;
    int fileIndex = 1; // one-based!
    for (File datasetFile : filesPerDataset) {

      if (fileIndex >= from && fileIndex <= to) {

        // Get the current status.
        final String fileName = datasetFile.getName();
        final FileStatus fileStatus = Optional.ofNullable(mongoDao.getFileStatus(fileName))
            .orElse(new FileStatus(fileName, 0));
        final int linkCount = getLinkCount(datasetFile);

        // maintain statistics
        final boolean isFinished =
            fileStatus.isEndOfFileReached() || fileStatus.getLineReached() >= linkCount;
        totalLinkCount += linkCount;
        processedLinkCount += (isFinished ? linkCount : fileStatus.getLineReached());

        // Print
        if (!isFinished) {
          System.out.println(String
              .format("File %s: %s, line %s of %s. %s lines left.", fileIndex, fileName,
                  fileStatus.getLineReached(), linkCount, linkCount - fileStatus.getLineReached()));
        }
      }

      // Next file.
      fileIndex++;
    }

    LOGGER.info("{} files examined. Processed {} of {} links. {} links left.", fileIndex,
        processedLinkCount, totalLinkCount, totalLinkCount - processedLinkCount);
  }


}
