package eu.europeana.metis.technical.metadata.generation.utilities;

import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.model.TechnicalMetadataWrapper;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailWrapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.mongodb.morphia.Datastore;

/**
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-04-17
 */
public class MongoDao {

  private static final String ID = "_id";
  private static final String RESOURCE_URL = "resourceUrl";
  private static final String FILE_NAME = "fileName";

  private Datastore datastore;

  MongoDao(Datastore datastore) {
    this.datastore = datastore;
  }

  FileStatus getFileStatus(String fileName) {
    return datastore.find(FileStatus.class).filter(FILE_NAME, fileName).get();
  }

  void storeFileStatusToDb(FileStatus fileStatus) {
    datastore.save(fileStatus);
  }

  boolean doesMediaAlreadyExist(String resourceUrl) {
    return datastore.find(TechnicalMetadataWrapper.class)
        .filter(RESOURCE_URL, resourceUrl).project(ID, true).get() != null;
  }

  void storeMediaResultInDb(ResourceExtractionResult resourceExtractionResult)
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

  void storeFailedMediaInDb(String resourceUrl) {
    //Keep track of the failed ones to bypass them on a second execution if needed
    final TechnicalMetadataWrapper technicalMetadataWrapper = new TechnicalMetadataWrapper();
    technicalMetadataWrapper.setResourceUrl(resourceUrl);
    technicalMetadataWrapper.setSuccessExtraction(false);
    datastore.save(technicalMetadataWrapper);
  }
}
