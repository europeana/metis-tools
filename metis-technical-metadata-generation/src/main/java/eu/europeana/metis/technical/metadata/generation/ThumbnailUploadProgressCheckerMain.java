package eu.europeana.metis.technical.metadata.generation;

import eu.europeana.metis.technical.metadata.generation.model.FileStatus;
import eu.europeana.metis.technical.metadata.generation.model.ThumbnailFileStatus;
import eu.europeana.metis.technical.metadata.generation.utilities.MongoDao;
import eu.europeana.metis.technical.metadata.generation.utilities.MongoInitializer;
import eu.europeana.metis.technical.metadata.generation.utilities.PropertiesHolder;
import eu.europeana.metis.utils.CustomTruststoreAppender.TrustStoreConfigurationException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.mongodb.morphia.Datastore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main method to manually check progress of the execution of the script with main class {@link
 * TechnicalMetadataGenerationMain} and mode {@link eu.europeana.metis.technical.metadata.generation.model.Mode#UPLOAD_THUMBNAILS}
 * <p>It compares {@link FileStatus} and {@link ThumbnailFileStatus} from the database.</p>
 *
 * @author Simon Tzanakis (Simon.Tzanakis@europeana.eu)
 * @since 2019-06-06
 */
public class ThumbnailUploadProgressCheckerMain {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ThumbnailUploadProgressCheckerMain.class);
  private static final String CONFIGURATION_FILE = "application.properties";

  public static void main(String[] args) throws TrustStoreConfigurationException {
    // Initialize.
    LOGGER.info("Starting script - initializing connections.");
    final PropertiesHolder propertiesHolder = new PropertiesHolder(CONFIGURATION_FILE);
    final MongoInitializer mongoInitializer = TechnicalMetadataGenerationMain
        .prepareConfiguration(propertiesHolder);
    final Datastore datastore = TechnicalMetadataGenerationMain
        .createDatastore(mongoInitializer.getMongoClient(), propertiesHolder.mongoDb);

    checkAll(datastore);
  }

  private static void checkAll(Datastore datastore) {
    final MongoDao mongoDao = new MongoDao(datastore);
    final List<FileStatus> allFileStatus = mongoDao.getAllFileStatus();
    final List<ThumbnailFileStatus> allThumbnailFileStatus = mongoDao.getAllThumbnailFileStatus();

    AtomicInteger totalFinished = new AtomicInteger();
    AtomicInteger totalFinishedNeedRerun = new AtomicInteger();
    AtomicInteger totalUnfinished = new AtomicInteger();
    AtomicInteger totalBypassed = new AtomicInteger();
    AtomicInteger totalNotExistsYet = new AtomicInteger();
    AtomicInteger totalNotStartedYet = new AtomicInteger();
    allFileStatus.forEach(fileStatus -> {
      final ThumbnailFileStatus thumbnailFileStatusInList = allThumbnailFileStatus.stream().filter(
          thumbnailFileStatus -> thumbnailFileStatus.getFileName()
              .equals(fileStatus.getFileName())).findFirst().orElse(null);
      final Status status = displayRelevantLogInfo(fileStatus, thumbnailFileStatusInList);
      updateCounts(status, totalFinished, totalFinishedNeedRerun, totalUnfinished,
          totalNotStartedYet, totalBypassed, totalNotExistsYet);
    });
    LOGGER.info(
        "TotalFinished: {}, TotalFinishedNeedRerun: {}, TotalUnFinished: {}, TotalNotStartedYet {}, "
            + "TotalBypassed: {}, TotalNotExistsYet: {}",
        totalFinished, totalFinishedNeedRerun, totalUnfinished, totalNotStartedYet,
        totalBypassed, totalNotExistsYet);
    LOGGER.info("TotalFileStatuses: {}, TotalThumbnailFileStatuses: {}", allFileStatus.size(),
        allThumbnailFileStatus.size());
  }

  private static void updateCounts(Status status, AtomicInteger totalFinished,
      AtomicInteger totalFinishedNeedRerun, AtomicInteger totalUnfinished,
      AtomicInteger totalNotStartedYet, AtomicInteger totalBypassed,
      AtomicInteger totalNotExistsYet) {
    switch (status) {
      case FINISHED:
        totalFinished.incrementAndGet();
        break;
      case FINISHED_NEEDS_RERUN:
        totalFinishedNeedRerun.incrementAndGet();
        break;
      case UNFINISHED:
        totalUnfinished.incrementAndGet();
        break;
      case DID_NOT_START_YET:
        totalNotStartedYet.incrementAndGet();
        break;
      case BYPASSED:
        totalBypassed.incrementAndGet();
        break;
      case NOT_EXISTS_YET:
        totalNotExistsYet.incrementAndGet();
        break;
      default:
        break;
    }
  }

  private static Status displayRelevantLogInfo(FileStatus fileStatus,
      ThumbnailFileStatus thumbnailFileStatusInList) {
    Status status;
    if (thumbnailFileStatusInList != null) {
      if (thumbnailFileStatusInList.isEndOfFileReached() && fileStatus.isEndOfFileReached()) {
        //ThumbnailFileStatus and FileStatus reached EOF
        status = Status.FINISHED;
      } else if (thumbnailFileStatusInList.isEndOfFileReached() && !fileStatus
          .isEndOfFileReached()) {
        //ThumbnailFileStatus reached EOF but fileStatus has not, this dataset must be re-run
        status = Status.FINISHED_NEEDS_RERUN;
      } else if (!thumbnailFileStatusInList.isEndOfFileReached()
          && thumbnailFileStatusInList.getLineReached() == 0 && fileStatus
          .isEndOfFileReached()) {
        //ThumbnailFileStatus reached EOF but fileStatus has not, this dataset must be re-run
        status = Status.DID_NOT_START_YET;
      } else if (!thumbnailFileStatusInList.isEndOfFileReached()
          && thumbnailFileStatusInList.getLineReached() != 0 && fileStatus
          .isEndOfFileReached()) {
        //ThumbnailFileStatus has not yet reached EOF
        status = Status.UNFINISHED;
      } else {
        status = Status.BYPASSED;
      }
      LOGGER.info("{} - FileStatus: {} - ThumbnailFileStatus: {}", status.name(), fileStatus,
          thumbnailFileStatusInList);
    } else {
      status = Status.NOT_EXISTS_YET;
      LOGGER.info("{} - FileStatus: {} - ThumbnailFileStatus does not 'yet' exist.", status.name(),
          fileStatus.getFileName());
    }
    return status;
  }

  private enum Status {
    NOT_EXISTS_YET, DID_NOT_START_YET, UNFINISHED, BYPASSED, FINISHED, FINISHED_NEEDS_RERUN
  }

}
