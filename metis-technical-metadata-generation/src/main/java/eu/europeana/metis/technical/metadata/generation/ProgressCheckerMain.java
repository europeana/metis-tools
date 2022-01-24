package eu.europeana.metis.technical.metadata.generation;

import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.utilities.ExecutorManager;
import eu.europeana.metis.technical.metadata.generation.utilities.MediaExtractorForFile;
import eu.europeana.metis.technical.metadata.generation.utilities.MongoDao;
import eu.europeana.metis.technical.metadata.generation.utilities.MongoInitializer;
import eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProgressCheckerMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProgressCheckerMain.class);

  private static final String CONFIGURATION_FILE = "application.properties";

  public static void main(String[] args) throws IOException, TrustStoreConfigurationException {

    // Initialize.
    LOGGER.info("Starting script - initializing connections.");
    final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);
    final MongoInitializer mongoInitializer = TechnicalMetadataGenerationMain
        .prepareConfiguration(propertiesHolder);
    final Datastore datastore = TechnicalMetadataGenerationMain
        .createDatastore(mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);

    // Check the progress.
    System.out.println("FIRST PASS");

    System.out.println("RECHECK");
    checkProgress(propertiesHolder, datastore, 181, 181);
    checkProgress(propertiesHolder, datastore, 182, 185);
    checkProgress(propertiesHolder, datastore, 1088, 1088);
    checkProgress(propertiesHolder, datastore, 1981, 2200);

    System.out.println("COMPLETED");
    checkProgress(propertiesHolder, datastore, 1, 180);
    checkProgress(propertiesHolder, datastore, 186, 1087);
    checkProgress(propertiesHolder, datastore, 1089, 1980);

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

  private static void checkProgress(PropertiesHolder propertiesHolder, Datastore datastore,
      int from, int to) throws IOException {
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
