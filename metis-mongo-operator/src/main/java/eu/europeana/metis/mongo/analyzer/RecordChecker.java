package eu.europeana.metis.mongo.analyzer;

import static eu.europeana.metis.mongo.analyzer.utilities.RecordIdsHelper.getRecordIds;

import com.mongodb.client.MongoClient;
import com.mongodb.lang.Nullable;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.corelib.web.exception.EuropeanaException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks whether a specific record or a list of records are readable by using morphia and {@link
 * EdmMongoServerImpl}
 */
public class RecordChecker implements Operator {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordChecker.class);

  private final EdmMongoServerImpl edmMongoServer;
  private final long counterCheckpoint;
  private final String recordAboutToCheck;
  private final Path pathWithCorruptedRecords;

  public RecordChecker(MongoClient mongoClient, String databaseName, long counterCheckpoint,
      @Nullable String recordAboutToCheck, String filePathWithCorruptedRecords) {
    this.edmMongoServer = new EdmMongoServerImpl(mongoClient, databaseName, false);
    this.counterCheckpoint = counterCheckpoint;
    this.pathWithCorruptedRecords = Paths.get(filePathWithCorruptedRecords);
    this.recordAboutToCheck = recordAboutToCheck;
  }

  public void operate() {
    List<String> recordAbouts = getRecordIds(recordAboutToCheck, pathWithCorruptedRecords);
    checkReadOfCorruptedRecord(edmMongoServer, recordAbouts);
  }

  private void checkReadOfCorruptedRecord(EdmMongoServerImpl edmMongoServer,
      List<String> recordAbouts) {
    int counterFailures = 0;
    int counter = 0;
    for (String about : recordAbouts) {
      try {
        edmMongoServer.getFullBean(about);
      } catch (EuropeanaException e) {
        LOGGER.debug("Cannot read record with about {}", about);
        counterFailures++;
      }

      counter++;
      if (counter % counterCheckpoint == 0 && LOGGER.isInfoEnabled()) {
        LOGGER.info("Reconstructed {} records from collection {}", counter, "record");
        LOGGER.info("Unsuccessful read(s): {}", counterFailures);
      }
    }
    LOGGER.info("Checked {} records from collection {}", counter, "record");
    LOGGER.info("Successful read(s): {}, Unsuccessful read(s): {}",
        recordAbouts.size() - counterFailures, counterFailures);
  }

}
