package eu.europeana.metis.technical.metadata.generation.utilities;

import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.model.PhysicalFileStatus;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailFileStatus;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.mongodb.morphia.Datastore;

/**
 * Mongo functionality required for the current script.
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class MongoDao {

  private static final String ID = "_id";
  private static final String RESOURCE_URL = "resourceUrl";
  private static final String FILE_NAME = "fileName";
  private static final String SUCCESS_EXTRACTION = "successExtraction";

  private Datastore datastore;

  public MongoDao(Datastore datastore) {
    this.datastore = datastore;
  }

  public FileStatus getFileStatus(String fileName) {
    return datastore.find(FileStatus.class).filter(FILE_NAME, fileName).get();
  }

  public List<FileStatus> getAllFileStatus() {
    return datastore.find(FileStatus.class).asList();
  }

  public ThumbnailFileStatus getThumbnailFileStatus(String fileName) {
    return datastore.find(ThumbnailFileStatus.class).filter(FILE_NAME, fileName).get();
  }

  public List<ThumbnailFileStatus> getAllThumbnailFileStatus() {
    return datastore.find(ThumbnailFileStatus.class).asList();
  }

  public void storeFileStatusToDb(PhysicalFileStatus fileStatus) {
    datastore.save(fileStatus);
  }

  TechnicalMetadataWrapper getTechnicalMetadataWrapperFieldProjection(String resourceUrl) {
    return datastore.find(TechnicalMetadataWrapper.class)
        .filter(RESOURCE_URL, resourceUrl).project(ID, true).project(SUCCESS_EXTRACTION, true)
        .get();
  }

  TechnicalMetadataWrapper getTechnicalMetadataWrapper(String resourceUrl) {
    return datastore.find(TechnicalMetadataWrapper.class)
        .field(RESOURCE_URL).equal(resourceUrl).get();
  }

  void removeThumbnailsFromTechnicalMetadataWrapper(
      TechnicalMetadataWrapper technicalMetadataWrapper) {
    technicalMetadataWrapper.setThumbnailWrappers(null);
    datastore.save(technicalMetadataWrapper);
  }

  void storeMediaResultInDb(ResourceExtractionResult resourceExtractionResult)
      throws IOException {

    if (resourceExtractionResult == null || resourceExtractionResult.getMetadata() == null) {
      return;
    }

    final TechnicalMetadataWrapper technicalMetadataWrapper = new TechnicalMetadataWrapper();
    technicalMetadataWrapper
        .setResourceUrl(resourceExtractionResult.getMetadata().getResourceUrl());
    technicalMetadataWrapper.setResourceMetadata(resourceExtractionResult.getMetadata());

    List<ThumbnailWrapper> thumbnailWrappers = new ArrayList<>(2);
    if (resourceExtractionResult.getThumbnails() != null) {
      for (Thumbnail thumbnail : resourceExtractionResult.getThumbnails()) {
        final ThumbnailWrapper thumbnailWrapper = new ThumbnailWrapper();
        thumbnailWrapper.setTargetName(thumbnail.getTargetName());
        thumbnailWrapper.setThumbnailBytes(IOUtils.toByteArray(thumbnail.getContentStream()));
        thumbnailWrappers.add(thumbnailWrapper);
      }
    }
    technicalMetadataWrapper.setThumbnailWrappers(thumbnailWrappers);
    technicalMetadataWrapper.setSuccessExtraction(true);

    datastore.updateFirst(datastore.find(TechnicalMetadataWrapper.class)
            .filter(RESOURCE_URL, technicalMetadataWrapper.getResourceUrl()), technicalMetadataWrapper,
        true);
  }

  void storeFailedMediaInDb(String resourceUrl, String errorTrace) {
    //Keep track of the failed ones to bypass them on a second execution if needed
    TechnicalMetadataWrapper technicalMetadataWrapper = getTechnicalMetadataWrapperFieldProjection(
        resourceUrl);
    if (technicalMetadataWrapper == null) {
      technicalMetadataWrapper = new TechnicalMetadataWrapper();
    }
    technicalMetadataWrapper.setResourceUrl(resourceUrl);
    technicalMetadataWrapper.setSuccessExtraction(false);
    technicalMetadataWrapper.setErrorStackTrace(errorTrace);
    datastore.save(technicalMetadataWrapper);
  }

  long getTotalFailedResources() {
    return datastore.find(TechnicalMetadataWrapper.class).filter(SUCCESS_EXTRACTION, false).count();
  }

  long getTotalProcessedResources() {
    return datastore.find(TechnicalMetadataWrapper.class).count();
  }
}
