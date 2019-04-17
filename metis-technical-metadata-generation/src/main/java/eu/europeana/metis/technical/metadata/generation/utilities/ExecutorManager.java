package eu.europeana.metis.technical.metadata.generation.utilities;

import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.mediaprocessing.model.UrlType;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailWrapper;
import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.TimeZone;
import org.apache.commons.io.IOUtils;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-16
 */
public class ExecutorManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorManager.class);
  private static final String RESOURCE_URL = "resourceUrl";
  private static final String ID = "_id";
  private static final String PLUGIN_STATUS = "pluginStatus";
  private static final String FINISHED_DATE = "finishedDate";
  private static final String UPDATED_DATE = "updatedDate";
  private final Datastore datastore;
  private final File directoryWithResourcesPerDataset;

  private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final DateFormat dateFormat;
  private final MediaExtractor mediaExtractor;

  static {
    dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public ExecutorManager(Datastore datastore, File directoryWithResourcesPerDataset)
      throws MediaProcessorException {
    this.datastore = datastore;
    this.directoryWithResourcesPerDataset = directoryWithResourcesPerDataset;
    final MediaProcessorFactory processorFactory = new MediaProcessorFactory();
    this.mediaExtractor = processorFactory.createMediaExtractor();
  }

  public void startTechnicalMetadataGeneration() throws IOException {
    //Is same file flag? Then continue from line stored in db, otherwise from the beginning of the file.

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
    int lineIndex = 0;
    try (Scanner scanner = new Scanner(datasetFile, "UTF-8")) {
      while (scanner.hasNextLine()) {
        String resourceUrl = scanner.nextLine();
        //Only generate if non existent
        if (!doesMediaAlreadyExist(resourceUrl)) {
          final ResourceExtractionResult resourceExtractionResult;
          try {
            resourceExtractionResult = performMediaExtraction(resourceUrl);
            storeMediaResultInDb(resourceExtractionResult);
            clearThumbnails(resourceExtractionResult);
          } catch (MediaExtractionException e) {
            LOGGER.warn("Media extraction failed for resourceUrl {}", resourceUrl);
            storeFailedMediaInDb(resourceUrl);
          }
        } else {
          LOGGER.info("ResourceUrl already exists in db: {}", resourceUrl);
        }
        lineIndex++;
      }
    }
  }

  private boolean doesMediaAlreadyExist(String resourceUrl) {
    return datastore.find(TechnicalMetadataWrapper.class)
        .filter(RESOURCE_URL, resourceUrl).project(ID, true).get() != null;
  }

  private void storeMediaResultInDb(ResourceExtractionResult resourceExtractionResult)
      throws IOException {

    final TechnicalMetadataWrapper technicalMetadataWrapper = new TechnicalMetadataWrapper();
    technicalMetadataWrapper
        .setResourceUrl(resourceExtractionResult.getMetadata().getResourceUrl());
    technicalMetadataWrapper.setResourceMetadata(resourceExtractionResult.getMetadata());

    List<ThumbnailWrapper> thumbnailWrappers = new ArrayList<>(2);
    for (Thumbnail thumbnail : resourceExtractionResult.getThumbnails()) {
      final ThumbnailWrapper thumbnailWrapper = new ThumbnailWrapper();
      thumbnailWrapper.setTargetName(thumbnail.getTargetName());
      thumbnailWrapper.setThumbnailBytes(IOUtils.toByteArray(thumbnail.getContentStream()));
      thumbnailWrappers.add(thumbnailWrapper);
    }
    technicalMetadataWrapper.setThumbnailWrappers(thumbnailWrappers);
    technicalMetadataWrapper.setSuccessExtraction(true);

    datastore.save(technicalMetadataWrapper);
  }

  private void storeFailedMediaInDb(String resourceUrl) {
    //Keep track of the failed ones to bypass them on a second execution if needed
    final TechnicalMetadataWrapper technicalMetadataWrapper = new TechnicalMetadataWrapper();
    technicalMetadataWrapper.setResourceUrl(resourceUrl);
    technicalMetadataWrapper.setSuccessExtraction(false);
    datastore.save(technicalMetadataWrapper);
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
