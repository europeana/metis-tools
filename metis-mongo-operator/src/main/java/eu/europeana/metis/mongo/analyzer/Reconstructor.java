package eu.europeana.metis.mongo.analyzer;

import static eu.europeana.metis.mongo.analyzer.utilities.RecordIdsHelper.getRecordIds;

import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.lang.Nullable;
import dev.morphia.Datastore;
import eu.europeana.corelib.mongo.server.impl.EdmMongoServerImpl;
import eu.europeana.metis.mongo.analyzer.utilities.RecordListFields;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Recostruct a specific record or a list of records provided.
 */
public class Reconstructor implements Operator {

  private static final Logger LOGGER = LoggerFactory.getLogger(Reconstructor.class);
  private static final String ABOUT_FIELD = "about";

  private final Datastore datastore;
  private final long counterCheckpoint;
  private final String recordAboutToCheck;
  private final Path pathWithCorruptedRecords;

  public Reconstructor(MongoClient mongoClient, String databaseName,
      @Nullable String recordAboutToCheck, final long counterCheckpoint,
      String filePathWithCorruptedRecords) {
    final EdmMongoServerImpl edmMongoServer = new EdmMongoServerImpl(mongoClient, databaseName,
        false);
    this.datastore = edmMongoServer.getDatastore();
    this.counterCheckpoint = counterCheckpoint;
    this.recordAboutToCheck = recordAboutToCheck;
    this.pathWithCorruptedRecords = Paths.get(filePathWithCorruptedRecords);
  }

  public void operate() {
    List<String> recordAbouts = getRecordIds(recordAboutToCheck, pathWithCorruptedRecords);
    final List<String> fieldListsToCheck = Arrays.stream(RecordListFields.values())
        .map(RecordListFields::getFieldName).collect(Collectors.toList());
    reconstructRecords(datastore, "record", recordAbouts, fieldListsToCheck);
  }

  private void reconstructRecords(Datastore datastore, String collection, List<String> recordAbouts,
      List<String> fieldListsToCheck) {
    final List<Document> aboutsQueries = recordAbouts.stream()
        .map(about -> new Document(ABOUT_FIELD, about)).collect(Collectors.toList());
    int counter = 0;
    for (Document aboutQuery : aboutsQueries) {
      final MongoCollection<Document> mongoCollection = datastore.getDatabase()
          .getCollection(collection);
      final FindIterable<Document> findIterable = mongoCollection.find(aboutQuery);
      reconstructDocuments(fieldListsToCheck, mongoCollection, findIterable);

      counter++;
      if (counter % counterCheckpoint == 0 && LOGGER.isInfoEnabled()) {
        LOGGER.info("Reconstructed {} records from collection {}", counter, collection);
      }
    }
    LOGGER.info("Reconstructed {} records from collection {}", counter, collection);
  }

  private void reconstructDocuments(List<String> fieldListsToCheck,
      MongoCollection<Document> mongoCollection, FindIterable<Document> findIterable) {
    try (MongoCursor<Document> cursor = findIterable.cursor()) {
      while (cursor.hasNext()) {
        final Document document = cursor.next();

        for (String fieldList : fieldListsToCheck) {
          Optional.ofNullable(document.get(fieldList)).map(o -> (List<DBRef>) o)
              .map(this::removeDuplicatesFromList)
              .ifPresent(dbRefsList -> document.put(fieldList, dbRefsList));
        }
        updateDocumentInDb(document, mongoCollection);
      }
    }
  }

  private void updateDocumentInDb(Document document, MongoCollection<Document> mongoCollection) {
    mongoCollection.replaceOne(new Document("_id", document.getObjectId("_id")), document);
  }

  private List<DBRef> removeDuplicatesFromList(List<DBRef> list) {
    return list.stream().distinct().collect(Collectors.toList());
  }

}
